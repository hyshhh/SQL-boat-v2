/**
 * Pipeline 前端逻辑 — 视频 Demo / 摄像头 Demo
 *
 * 视频 Demo：后端推理，实时 MJPEG 推流到前端，不保存输出视频
 * 摄像头 Demo：浏览器/服务器摄像头，实时推流识别
 */

const PIPE_API = '/api/pipeline';

// ── Tab 切换 ──
function switchTab(tabName) {
  document.querySelectorAll('.tab-btn').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.tab === tabName);
  });
  document.querySelectorAll('.tab-content').forEach(el => {
    el.classList.toggle('active', el.id === `tab-${tabName}`);
  });
  // 按需加载数据
  if (tabName === 'video-demo') {
    loadVideoList();
    loadTaskHistory();
  } else if (tabName === 'camera-demo') {
    onCameraSourceChange();
  } else if (tabName === 'database') {
    if (typeof loadShips === 'function') loadShips();
  }
}

// ═══════════════════════════════════════════
// 视频 Demo
// ═══════════════════════════════════════════

let selectedVideo = null;
let currentTaskId = null;
let statusPollTimer = null;
let logPollTimer = null;
let _logIndex = 0;           // 已拉取的日志索引
let streamWs = null;        // WebSocket 推流连接
let _h264Ws = null;          // H.264 WebSocket
let _h264MediaSource = null; // MediaSource
let _h264SourceBuffer = null;// SourceBuffer
let _h264Queue = [];         // 积压的 segment 队列

// ── 视频上传 ──
const videoUploadZone = document.getElementById('videoUploadZone');
const videoFileInput = document.getElementById('videoFileInput');

if (videoFileInput) {
  videoFileInput.addEventListener('change', function (e) {
    if (e.target.files.length > 0) handleVideoUpload(e.target.files[0]);
  });
}

if (videoUploadZone) {
  videoUploadZone.addEventListener('dragover', function (e) {
    e.preventDefault(); e.stopPropagation();
    this.classList.add('dragover');
  });
  videoUploadZone.addEventListener('dragleave', function (e) {
    e.preventDefault(); e.stopPropagation();
    this.classList.remove('dragover');
  });
  videoUploadZone.addEventListener('drop', function (e) {
    e.preventDefault(); e.stopPropagation();
    this.classList.remove('dragover');
    if (e.dataTransfer.files.length > 0) handleVideoUpload(e.dataTransfer.files[0]);
  });
}

async function handleVideoUpload(file) {
  const allowedExts = ['.mp4', '.avi', '.mkv', '.mov', '.flv', '.wmv', '.webm'];
  const ext = '.' + file.name.split('.').pop().toLowerCase();
  if (!allowedExts.includes(ext)) {
    showToast('不支持的视频格式: ' + ext, 'error');
    return;
  }
  if (file.size > 500 * 1024 * 1024) {
    showToast('文件过大，最大 500MB', 'error');
    return;
  }

  document.getElementById('videoUploadFilename').textContent = file.name;
  const progressWrap = document.getElementById('videoUploadProgress');
  const progressBar = document.getElementById('videoProgressBar');
  const progressText = document.getElementById('videoProgressText');
  progressWrap.style.display = 'block';
  progressBar.style.width = '0%';
  progressText.textContent = '上传中...';

  try {
    const formData = new FormData();
    formData.append('file', file);

    const result = await new Promise((resolve, reject) => {
      const xhr = new XMLHttpRequest();
      xhr.open('POST', `${PIPE_API}/videos/upload`);

      xhr.upload.addEventListener('progress', function (e) {
        if (e.lengthComputable) {
          const pct = Math.round((e.loaded / e.total) * 100);
          progressBar.style.width = pct + '%';
          progressText.textContent = pct + '%';
        }
      });

      xhr.addEventListener('load', function () {
        if (xhr.status >= 200 && xhr.status < 300) {
          resolve(JSON.parse(xhr.responseText));
        } else {
          let msg = '上传失败';
          try { msg = JSON.parse(xhr.responseText).detail || msg; } catch {}
          reject(new Error(msg));
        }
      });

      xhr.addEventListener('error', () => reject(new Error('网络错误')));
      xhr.send(formData);
    });

    showToast(`✅ 视频已上传: ${result.filename}`);
    progressBar.style.width = '100%';
    progressText.textContent = '完成!';
    setTimeout(() => { progressWrap.style.display = 'none'; }, 2000);
    loadVideoList();
  } catch (e) {
    showToast('上传失败: ' + e.message, 'error');
    progressWrap.style.display = 'none';
  }

  videoFileInput.value = '';
}

// ── 视频列表 ──
async function loadVideoList() {
  const container = document.getElementById('videoList');
  if (!container) return;
  try {
    const resp = await fetch(`${PIPE_API}/videos`);
    const data = await resp.json();
    if (!data.videos.length) {
      container.innerHTML = '<div class="empty-msg">暂无视频，请上传</div>';
      return;
    }
    container.innerHTML = data.videos.map(v => `
      <div class="video-item ${selectedVideo === v.filename ? 'selected' : ''}"
           onclick="selectVideo(this.dataset.name, this)" data-name="${safeAttr(v.filename)}">
        <div class="video-item-icon">🎬</div>
        <div class="video-item-info">
          <div class="video-item-name">${escHtml(v.filename)}</div>
          <div class="video-item-meta">${v.size_mb} MB</div>
        </div>
        <div class="video-item-actions">
          <button class="btn btn-danger btn-sm" onclick="event.stopPropagation(); deleteVideo(this.dataset.name)" data-name="${safeAttr(v.filename)}">🗑️</button>
        </div>
      </div>
    `).join('');
  } catch (e) {
    container.innerHTML = `<div class="empty-msg">加载失败: ${e.message}</div>`;
  }
}

function selectVideo(filename, el) {
  // 如果有 pipeline 在运行，先提示用户
  if (currentTaskId) {
    if (!confirm('当前有 Pipeline 正在运行，切换视频将停止当前任务。是否继续？')) return;
    stopVideoPipeline();
  }

  selectedVideo = filename;
  document.getElementById('pipelineControl').style.display = '';
  // 更新选中状态
  document.querySelectorAll('.video-item').forEach(item => item.classList.remove('selected'));
  if (el) el.classList.add('selected');

  // 重置结果区域
  const resultPlaceholder = document.getElementById('resultPlaceholder');
  if (resultPlaceholder) {
    resultPlaceholder.innerHTML = '<span>🎬</span><p>点击"开始处理"后实时显示</p>';
    resultPlaceholder.className = 'video-placeholder';
    resultPlaceholder.style.cssText = '';
    resultPlaceholder.style.display = '';
  }
  resetPipelineStatus();
}

async function deleteVideo(filename) {
  if (!confirm(`确定删除视频 "${filename}"？`)) return;
  try {
    const resp = await fetch(`${PIPE_API}/videos/${encodeURIComponent(filename)}`, { method: 'DELETE' });
    const data = await resp.json();
    if (!resp.ok) throw new Error(data.detail || '删除失败');
    showToast('已删除: ' + filename);
    if (selectedVideo === filename) {
      selectedVideo = null;
      document.getElementById('pipelineControl').style.display = 'none';
    }
    loadVideoList();
  } catch (e) {
    showToast(e.message, 'error');
  }
}

// ── Pipeline 控制 ──

/** 收集视频 Demo 页的 pipeline 参数 */
function collectVideoParams() {
  return {
    conf_threshold: parseFloat(document.getElementById('optConf').value),
    iou_threshold: parseFloat(document.getElementById('optIou').value),
    process_every: parseInt(document.getElementById('optProcessEvery').value, 10),
    detect_every: parseInt(document.getElementById('optDetectEvery').value, 10),
    target_fps: parseFloat(document.getElementById('optTargetFps').value) || 0,
    pipe_scale: parseFloat(document.getElementById('optPipeScale').value) || 0.5,
    max_frames: parseInt(document.getElementById('optMaxFrames').value, 10) || 0,
    device: document.getElementById('optDevice').value,
    yolo_model: document.getElementById('optYoloModel').value.trim(),
    prompt_mode: document.getElementById('optPromptMode').value,
    enable_refresh: document.getElementById('optEnableRefresh').checked,
    gap_num: parseInt(document.getElementById('optGapNum').value, 10) || 150,
    max_concurrent: parseInt(document.getElementById('optMaxConcurrent').value, 10) || 4,
  };
}

/** 收集摄像头页的 pipeline 参数 */
function collectCameraParams() {
  return {
    conf_threshold: parseFloat(document.getElementById('camConf').value),
    iou_threshold: parseFloat(document.getElementById('camIou').value),
    process_every: parseInt(document.getElementById('camProcessEvery').value, 10),
    detect_every: parseInt(document.getElementById('camDetectEvery').value, 10),
    target_fps: parseFloat(document.getElementById('camTargetFps').value) || 0,
    capture_fps: parseInt(document.getElementById('camCaptureFps').value, 10) || 15,
    pipe_scale: parseFloat(document.getElementById('camPipeScale')?.value) || 0.5,
    max_frames: parseInt(document.getElementById('camMaxFrames').value, 10) || 0,
    device: document.getElementById('camDevice').value,
    yolo_model: document.getElementById('camYoloModel').value.trim(),
    prompt_mode: document.getElementById('camPromptMode').value,
    enable_refresh: document.getElementById('camEnableRefresh').checked,
    gap_num: parseInt(document.getElementById('camGapNum').value, 10) || 150,
    max_concurrent: parseInt(document.getElementById('camMaxConcurrent').value, 10) || 4,
    stream_mode: (document.getElementById('camStreamMode') || {}).value || 'mjpeg',
  };
}

async function startVideoPipeline() {
  if (!selectedVideo) { showToast('请先选择视频', 'error'); return; }

  const btn = document.getElementById('btnStartPipeline');
  btn.disabled = true;
  btn.innerHTML = '<span class="loading-spinner"></span> 启动中...';

  try {
    const resp = await fetch(`${PIPE_API}/start`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        video_filename: selectedVideo,
        concurrent_mode: document.getElementById('optConcurrent').checked,
        ...collectVideoParams(),
      }),
    });
    const data = await resp.json();
    if (!resp.ok) throw new Error(data.detail || '启动失败');

    currentTaskId = data.task_id;
    showToast(`Pipeline 已启动 (${currentTaskId})`);
    updatePipelineStatus('running', '处理中...');
    document.getElementById('btnStartPipeline').style.display = 'none';
    document.getElementById('btnStopPipeline').style.display = '';

    // 实时预览：H.264 WebSocket 推流 + MSE 播放
    const resultPlaceholder = document.getElementById('resultPlaceholder');
    if (resultPlaceholder) {
      resultPlaceholder.innerHTML = `
        <video id="streamVideo" class="demo-video" autoplay muted playsinline></video>
        <div id="streamFps" style="text-align:center;font-size:12px;color:#888;margin-top:4px">连接中...</div>
      `;
      resultPlaceholder.style.background = 'transparent';
      resultPlaceholder.style.border = 'none';
    }

    connectStreamWs(currentTaskId);
    startStatusPolling();
  } catch (e) {
    showToast('启动失败: ' + e.message, 'error');
  } finally {
    btn.disabled = false;
    btn.innerHTML = '▶ 开始处理';
  }
}

/** 建立 H.264 WebSocket 推流连接（MSE 播放） */
function connectStreamWs(taskId) {
  disconnectStreamWs();

  const wsProto = location.protocol === 'https:' ? 'wss' : 'ws';
  const wsUrl = `${wsProto}://${location.host}${PIPE_API}/ws/h264/${taskId}`;

  const videoEl = document.getElementById('streamVideo');
  if (!videoEl) return;

  // MediaSource
  const ms = new MediaSource();
  videoEl.src = URL.createObjectURL(ms);
  videoEl.load();  // 强制加载，确保 sourceopen 触发
  _h264MediaSource = ms;
  _h264SourceBuffer = null;
  _h264Queue = [];

  ms.addEventListener('sourceopen', () => {
    // 等 WebSocket 收到 init segment 后再添加 SourceBuffer
    const ws = new WebSocket(wsUrl);
    ws.binaryType = 'arraybuffer';
    streamWs = ws;
    _h264Ws = ws;

    let frameCount = 0;
    let fpsTimer = performance.now();

    /** 尝试播放（autoplay 可能被浏览器策略阻止） */
    function _ensurePlay() {
      const vEl = document.getElementById('streamVideo');
      if (vEl && vEl.paused) {
        vEl.play().catch(() => {
          // autoplay 被阻止，用户点击视频区域可手动播放
          vEl.muted = true;
          vEl.play().catch(() => {});
        });
      }
    }

    /** 处理队列积压 + 清理已播放缓冲区 */
    function _processQueue() {
      const sb = _h264SourceBuffer;
      if (!sb || sb.updating) return;

      // 清理已播放的旧缓冲区（保留播放位置前 3 秒）
      try {
        const vEl = document.getElementById('streamVideo');
        if (vEl && sb.buffered.length > 0 && sb.buffered.start(0) < vEl.currentTime - 8) {
          sb.remove(sb.buffered.start(0), vEl.currentTime - 3);
          return; // remove 完成后会再次触发 updateend
        }
      } catch (e) {}

      // 追加队列中下一个分片
      if (_h264Queue.length > 0) {
        try {
          sb.appendBuffer(_h264Queue.shift());
        } catch (e) {
          if (e.name === 'QuotaExceededError') {
            // 缓冲区仍满，清空队列并尝试强制清理
            _h264Queue.length = 0;
            try {
              const vEl = document.getElementById('streamVideo');
              if (vEl && sb.buffered.length > 0) {
                sb.remove(sb.buffered.start(0), vEl.currentTime);
              }
            } catch (e2) {}
          }
        }
      }
    }

    ws.onmessage = (evt) => {
      if (evt.data instanceof ArrayBuffer) {
        const view = new DataView(evt.data);
        const msgType = view.getUint8(0);
        const payload = evt.data.slice(5);

        if (msgType === 0x01) {
          // Init segment (moov) — 创建 SourceBuffer（仅首次）
          if (_h264SourceBuffer) {
            // 已有 SourceBuffer，直接追加 init 数据（用于重连后刷新解码器）
            if (!_h264SourceBuffer.updating) {
              try { _h264SourceBuffer.appendBuffer(payload); } catch (e) {}
            }
            return;
          }
          try {
            if (ms.readyState !== 'open') {
              console.warn('MediaSource 未就绪，忽略 init segment');
              return;
            }
            const codecs = 'avc1.42C01F'; // H.264 Constrained Baseline Level 3.1
            const sb = ms.addSourceBuffer(`video/mp4; codecs="${codecs}"`);
            _h264SourceBuffer = sb;

            sb.addEventListener('updateend', () => {
              _processQueue();
            });
            sb.addEventListener('error', (e) => {
              console.error('SourceBuffer 错误:', e);
            });

            sb.appendBuffer(payload);
            _ensurePlay();  // init segment 就绪后尝试播放
          } catch (e) {
            console.error('MSE SourceBuffer 创建失败:', e);
          }

        } else if (msgType === 0x02) {
          // Media segment (moof+mdat)
          const sb = _h264SourceBuffer;
          if (!sb) return;

          if (sb.updating) {
            // 队列满了保留最新的一半，防止全部丢光导致黑屏
            if (_h264Queue.length >= 12) {
              _h264Queue = _h264Queue.slice(-6);
            }
            _h264Queue.push(payload);
          } else {
            try {
              sb.appendBuffer(payload);
              if (frameCount === 0) _ensurePlay();  // 首帧到达后尝试播放
            } catch (e) {
              if (e.name === 'QuotaExceededError') {
                // 缓冲区满，尝试清理后把当前帧和队列一起重试
                try {
                  const vEl = document.getElementById('streamVideo');
                  if (vEl && sb.buffered.length > 0) {
                    sb.remove(sb.buffered.start(0), vEl.currentTime - 2);
                  }
                } catch (e2) {}
                _h264Queue.unshift(payload);
              }
            }
          }

          // FPS 统计
          frameCount++;
          const now = performance.now();
          if (now - fpsTimer > 1000) {
            const segFps = (frameCount * 1000 / (now - fpsTimer)).toFixed(1);
            const fpsEl = document.getElementById('streamFps');
            if (fpsEl) fpsEl.textContent = `${segFps} seg/s`;
            frameCount = 0;
            fpsTimer = now;
          }
        }
      } else {
        // JSON 控制消息
        try {
          const msg = JSON.parse(evt.data);
          if (msg.type === 'done') {
            disconnectStreamWs();
            const fpsEl = document.getElementById('streamFps');
            if (fpsEl) fpsEl.textContent = '处理完成';
          }
        } catch {}
      }
    };

    ws.onclose = () => {
      if (currentTaskId === taskId) {
        _scheduleReconnect('h264-stream', () => {
          if (currentTaskId === taskId) connectStreamWs(taskId);
        }, taskId);
      }
    };

    ws.onerror = () => {};
  });
}

/** 断开 H.264 推流 */
function disconnectStreamWs() {
  _clearReconnect('h264-stream');
  if (_h264Ws) {
    _h264Ws.onclose = null;
    _h264Ws.close();
    _h264Ws = null;
  }
  if (streamWs) {
    streamWs.onclose = null;
    streamWs.close();
    streamWs = null;
  }
  // 释放 MediaSource
  if (_h264MediaSource && _h264MediaSource.readyState === 'open') {
    try { _h264MediaSource.endOfStream(); } catch {}
  }
  _h264MediaSource = null;
  _h264SourceBuffer = null;
  _h264Queue = [];

  const videoEl = document.getElementById('streamVideo');
  if (videoEl) {
    videoEl.pause();
    videoEl.src = '';
  }
}

async function stopVideoPipeline() {
  if (!currentTaskId) return;
  const taskId = currentTaskId;

  // 立即停止轮询，防止后续 pollTaskStatus 干扰新任务
  stopStatusPolling();
  currentTaskId = null;
  _logIndex = 0;

  // 清空识别日志
  const logContent = document.getElementById('pipelineLogContent');
  if (logContent) logContent.innerHTML = '';

  // 断开 WebSocket 推流
  disconnectStreamWs();

  // 更新 UI 状态
  updatePipelineStatus('failed', '正在停止...');
  resetPipelineButtons();

  try {
    const resp = await fetch(`${PIPE_API}/stop/${taskId}`, { method: 'POST' });
    if (resp.ok || resp.status === 404) {
      showToast('已停止');
    } else {
      const data = await resp.json().catch(() => ({}));
      showToast('停止: ' + (data.message || '完成'), 'info');
    }
  } catch (e) {
    showToast('已停止', 'info');
  }

  // 恢复结果占位
  _restoreResultPlaceholder();

  loadTaskHistory();
}

function startStatusPolling() {
  stopStatusPolling();
  statusPollTimer = setInterval(pollTaskStatus, 2000);
  // 启动日志轮询
  _logIndex = 0;
  const logBox = document.getElementById('pipelineLogBox');
  if (logBox) logBox.style.display = '';
  const logContent = document.getElementById('pipelineLogContent');
  if (logContent) logContent.innerHTML = '';
  logPollTimer = setInterval(pollPipelineLogs, 1500);
}

function stopStatusPolling() {
  if (statusPollTimer) {
    clearInterval(statusPollTimer);
    statusPollTimer = null;
  }
  if (logPollTimer) {
    clearInterval(logPollTimer);
    logPollTimer = null;
  }
}

async function pollTaskStatus() {
  // 快照当前任务 ID，防止请求返回时 currentTaskId 已变为新任务
  const taskId = currentTaskId;
  if (!taskId) return;
  try {
    const resp = await fetch(`${PIPE_API}/status/${taskId}`);
    if (resp.status === 404) {
      if (currentTaskId === taskId) {
        stopStatusPolling();
        resetPipelineButtons();
        currentTaskId = null;
      }
      return;
    }
    const data = await resp.json();
    updatePipelineStatus(data.status, data.progress || data.error || '');

    if (data.status === 'completed') {
      if (currentTaskId === taskId) {
        stopStatusPolling();
        resetPipelineButtons();
        disconnectStreamWs();
        showToast('✅ 处理完成!');
        const resultPlaceholder = document.getElementById('resultPlaceholder');
        if (resultPlaceholder) {
          resultPlaceholder.innerHTML = '<span>✅</span><p>处理完成</p>';
          resultPlaceholder.className = 'video-placeholder';
          resultPlaceholder.style.cssText = '';
        }
        loadTaskHistory();
        currentTaskId = null;
      }
    } else if (data.status === 'failed') {
      if (currentTaskId === taskId) {
        stopStatusPolling();
        resetPipelineButtons();
        disconnectStreamWs();
        _restoreResultPlaceholder();
        const errorMsg = data.error || '未知错误';
        if (errorMsg === '用户手动停止') {
          showToast('已停止', 'info');
        } else {
          showToast('处理失败: ' + errorMsg, 'error');
        }
        loadTaskHistory();
        currentTaskId = null;
      }
    }
  } catch (e) {
    console.error('状态轮询失败:', e);
  }
}

async function pollPipelineLogs() {
  const taskId = currentTaskId;
  if (!taskId) return;
  try {
    const resp = await fetch(`${PIPE_API}/logs/${taskId}?since=${_logIndex}`);
    if (!resp.ok) return;
    const data = await resp.json();
    if (data.logs && data.logs.length > 0) {
      const box = document.getElementById('pipelineLogContent');
      if (!box) return;
      const levelColors = { exact: '#4caf50', semantic: '#ff9800', miss: '#f44336' };
      // log_start 表示后端发生过 FIFO 清理，前端需要重置
      if (data.log_start !== undefined) {
        box.innerHTML = '';
      }
      for (const entry of data.logs) {
        const level = entry.level || 'miss';
        const color = levelColors[level] || '#f44336';
        const div = document.createElement('div');
        div.className = 'log-entry';
        div.innerHTML = `<span class="log-time">${entry.time}</span><span style="color:${color}">${entry.line}</span>`;
        box.appendChild(div);
      }
      _logIndex = data.total;
      box.scrollTop = box.scrollHeight;
    }
  } catch (e) {}
}

function updatePipelineStatus(status, text) {
  const dot = document.querySelector('#pipelineStatus .status-dot');
  const statusText = document.getElementById('pipelineStatusText');
  if (!dot || !statusText) return;
  dot.className = 'status-dot ' + (status === 'running' ? 'running' : status === 'completed' ? 'completed' : status === 'failed' ? 'failed' : 'idle');
  statusText.textContent = text || status;
}

function resetPipelineStatus() {
  updatePipelineStatus('idle', '等待开始');
  resetPipelineButtons();
}

function resetPipelineButtons() {
  const startBtn = document.getElementById('btnStartPipeline');
  const stopBtn = document.getElementById('btnStopPipeline');
  if (startBtn) startBtn.style.display = '';
  if (stopBtn) stopBtn.style.display = 'none';
}

/** 恢复结果区域为初始占位状态 */
function _restoreResultPlaceholder() {
  const resultPlaceholder = document.getElementById('resultPlaceholder');
  if (resultPlaceholder) {
    resultPlaceholder.innerHTML = '<span>🎬</span><p>点击"开始处理"后实时显示</p>';
    resultPlaceholder.className = 'video-placeholder';
    resultPlaceholder.style.cssText = '';
  }
  const logBox = document.getElementById('pipelineLogBox');
  if (logBox) logBox.style.display = 'none';
}

// ── 任务历史 ──
async function loadTaskHistory() {
  const container = document.getElementById('taskHistory');
  if (!container) return;
  try {
    const resp = await fetch(`${PIPE_API}/status`);
    const data = await resp.json();
    if (!data.tasks.length) {
      container.innerHTML = '<div class="empty-msg">暂无任务</div>';
      return;
    }
    container.innerHTML = data.tasks.map(t => {
      const statusIcon = t.status === 'completed' ? '✅' : t.status === 'running' ? '⏳' : '❌';
      const statusClass = t.status === 'completed' ? 'success' : t.status === 'running' ? 'running' : 'error';
      const cameraTag = t.is_camera ? ' <span style="color:#f57c00;font-size:12px">[摄像头]</span>' : '';
      return `
        <div class="task-item ${statusClass}">
          <div class="task-icon">${statusIcon}</div>
          <div class="task-info">
            <div class="task-name">${escHtml(t.video_filename)}${cameraTag}</div>
            <div class="task-meta">
              任务 ${escHtml(t.task_id)} · ${escHtml(t.progress || t.error || t.status)}
            </div>
          </div>
          <div class="task-actions">
            ${t.status === 'running' ? `<button class="btn btn-danger btn-sm" onclick="stopTaskById(this.dataset.id)" data-id="${safeAttr(t.task_id)}">⏹ 停止</button>` : ''}
          </div>
        </div>
      `;
    }).join('');
  } catch (e) {
    container.innerHTML = `<div class="empty-msg">加载失败: ${e.message}</div>`;
  }
}

async function clearTaskHistory() {
  try {
    const resp = await fetch(`${PIPE_API}/tasks/clear`, { method: 'DELETE' });
    const data = await resp.json();
    if (!resp.ok) throw new Error(data.detail || '清空失败');
    showToast(data.message || '已清空');
    loadTaskHistory();
  } catch (e) {
    showToast('清空失败: ' + e.message, 'error');
  }
}

async function stopTaskById(taskId) {
  try {
    await fetch(`${PIPE_API}/stop/${taskId}`, { method: 'POST' });
    showToast('已停止');
    loadTaskHistory();
  } catch (e) {
    showToast(e.message, 'error');
  }
}

// ═══════════════════════════════════════════
// 摄像头 Demo
// ═══════════════════════════════════════════

let cameraTaskId = null;
let cameraPollTimer = null;
let browserCameraStream = null;   // MediaStream
let browserCameraWs = null;       // WebSocket
let browserCameraTimer = null;    // 帧捕获定时器
let browserCameraCanvas = null;   // 离屏 canvas
let browserCameraMediaRecorder = null; // H264 MediaRecorder
let browserCameraCaptureFps = 15; // 推帧帧率

function onCameraSourceChange() {
  const sel = document.getElementById('cameraSource');
  if (!sel) return;
  const val = sel.value;
  const urlInput = document.getElementById('cameraUrl');
  const previewRow = document.getElementById('browserCameraPreviewRow');
  const streamModeRow = document.getElementById('camStreamModeRow');
  const streamModeHint = document.getElementById('camStreamModeHint');

  if (urlInput) {
    urlInput.style.display = (val === '0' || val === 'browser') ? 'none' : '';
    if (val === 'rtsp') {
      urlInput.placeholder = 'rtsp://192.168.1.100/stream';
    } else if (val === 'custom') {
      urlInput.placeholder = '输入视频路径或 URL';
    }
  }

  if (previewRow) {
    previewRow.style.display = val === 'browser' ? '' : 'none';
  }

  // H264/MJPEG 切换仅对浏览器摄像头可见；非浏览器时显示提示
  const isBrowser = val === 'browser';
  if (streamModeRow) streamModeRow.style.display = isBrowser ? '' : 'none';
  if (streamModeHint) streamModeHint.style.display = isBrowser ? 'none' : '';
}

function getCameraInput() {
  const sel = document.getElementById('cameraSource');
  if (!sel) return '';
  if (sel.value === '0') return '0';
  if (sel.value === 'browser') return '__browser__';
  const urlInput = document.getElementById('cameraUrl');
  return urlInput ? urlInput.value.trim() : '';
}

// ── 浏览器摄像头：启动 ──
async function startBrowserCamera() {
  const btn = document.getElementById('btnStartCamera');
  btn.disabled = true;
  btn.innerHTML = '<span class="loading-spinner"></span> 启动中...';

  const streamMode = (document.getElementById('camStreamMode') || {}).value || 'mjpeg';

  try {
    if (!navigator.mediaDevices || !navigator.mediaDevices.getUserMedia) {
      throw new Error('当前页面不是安全上下文（需要 HTTPS 或 localhost），浏览器不允许访问摄像头');
    }
    const stream = await navigator.mediaDevices.getUserMedia({
      video: { width: { ideal: 1280 }, height: { ideal: 720 }, facingMode: 'environment' },
      audio: false,
    }).catch(err => {
      if (err.name === 'NotAllowedError') throw new Error('摄像头权限被拒绝，请在浏览器弹窗中点击"允许"');
      if (err.name === 'NotFoundError') throw new Error('未检测到摄像头设备，请确认电脑有可用摄像头');
      if (err.name === 'NotReadableError') throw new Error('摄像头被其他程序占用，请关闭其他使用摄像头的应用');
      throw new Error('摄像头访问失败: ' + err.message);
    });
    browserCameraStream = stream;

    const preview = document.getElementById('browserCameraPreview');
    const placeholder = document.getElementById('browserCameraPreviewPlaceholder');
    if (preview) {
      preview.srcObject = stream;
      preview.style.display = '';
    }
    if (placeholder) placeholder.style.display = 'none';

    const params = collectCameraParams();
    const resp = await fetch(`${PIPE_API}/start-browser-camera`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        concurrent_mode: document.getElementById('camOptConcurrent').checked,
        stream_mode: streamMode,
        ...params,
      }),
    });
    const data = await resp.json();
    if (!resp.ok) throw new Error(data.detail || '启动失败');

    cameraTaskId = data.task_id;
    browserCameraCaptureFps = data.capture_fps || 15;

    const wsProto = location.protocol === 'https:' ? 'wss' : 'ws';
    const wsUrl = `${wsProto}://${location.host}${PIPE_API}/ws/camera/${cameraTaskId}`;

    if (streamMode === 'webrtc') {
      // ── WebRTC 模式：浏览器直连服务器，超低延迟 ──
      setupWebRTCCamera(cameraTaskId, stream);
    } else if (streamMode === 'h264') {
      // ── H264 模式：MediaRecorder 编码 → WebSocket ──
      setupH264CameraWs(wsUrl, stream);
    } else {
      // ── MJPEG 模式：逐帧 JPEG → WebSocket ──
      setupMjpegCameraWs(wsUrl, stream);
    }

  } catch (e) {
    showToast('启动失败: ' + e.message, 'error');
    stopBrowserCamera();
  } finally {
    btn.disabled = false;
    btn.innerHTML = '▶ 启动摄像头识别';
  }
}

/** MJPEG 模式：逐帧 JPEG 推流 */
function setupMjpegCameraWs(wsUrl, stream) {
  function setupWsHandlers(ws) {
    ws.onopen = () => {
      showToast('摄像头已连接 (MJPEG)，开始推流');
      updateCameraStatus('running', 'MJPEG 推流中...');
      document.getElementById('btnStartCamera').style.display = 'none';
      document.getElementById('btnStopCamera').style.display = '';

      connectCameraH264(cameraTaskId);
      startFrameCapture(ws, stream);
      startCameraPolling();
    };

    ws.onmessage = (evt) => {
      try {
        const msg = JSON.parse(evt.data);
        if (!msg.ok) console.warn('帧发送失败:', msg.error);
      } catch {}
    };

    ws.onerror = () => { console.warn('MJPEG WebSocket 错误'); };

    ws.onclose = (evt) => {
      if (!cameraTaskId) return;
      if (evt.code !== 1000) {
        showToast('摄像头连接断开，尝试重连…', 'info');
        _scheduleReconnect('mjpeg-cam', () => {
          if (!cameraTaskId) return;
          const newWs = new WebSocket(wsUrl);
          browserCameraWs = newWs;
          setupWsHandlers(newWs);
        }, cameraTaskId);
      }
    };
  }

  const ws = new WebSocket(wsUrl);
  browserCameraWs = ws;
  setupWsHandlers(ws);
}

/** H264 模式：MediaRecorder 编码 → WebSocket 推流 */
function setupH264CameraWs(wsUrl, stream) {
  // 检查 H264 MediaRecorder 支持 — 优先 avc1，fallback vp8
  const h264Mimes = [
    'video/mp4; codecs="avc1.42E01E"',  // Baseline 3.1
    'video/mp4; codecs="avc1.4D401E"',  // Main 3.1
    'video/mp4; codecs="avc1.64001E"',  // High 3.1
    'video/webm; codecs="h264"',        // WebM 容器 + H264
  ];
  const vp8Mime = 'video/webm; codecs="vp8"';

  let useMime = null;
  let codecName = null;

  for (const mime of h264Mimes) {
    if (typeof MediaRecorder !== 'undefined' && MediaRecorder.isTypeSupported(mime)) {
      useMime = mime;
      codecName = 'h264';
      break;
    }
  }
  if (!useMime && typeof MediaRecorder !== 'undefined' && MediaRecorder.isTypeSupported(vp8Mime)) {
    useMime = vp8Mime;
    codecName = 'vp8';
  }

  if (!useMime) {
    showToast('当前浏览器不支持 MediaRecorder 编码，已回退到 MJPEG 模式', 'info');
    setupMjpegCameraWs(wsUrl, stream);
    return;
  }

  console.log(`[H264 Camera] 使用 codec: ${codecName}, mime: ${useMime}`);
  showToast(`使用编码: ${useMime}`, 'info');

  const ws = new WebSocket(wsUrl);
  browserCameraWs = ws;

  ws.onopen = () => {
    showToast(`摄像头已连接 (${codecName.toUpperCase()})，开始推流`);
    updateCameraStatus('running', `${codecName.toUpperCase()} 推流中...`);
    document.getElementById('btnStartCamera').style.display = 'none';
    document.getElementById('btnStopCamera').style.display = '';

    // 结果推流：H264 MSE 播放
    connectCameraH264(cameraTaskId);
    startCameraPolling();

    // 首条消息：JSON 文本告知后端编码格式
    ws.send(JSON.stringify({ codec: codecName }));

    // 创建 MediaRecorder
    try {
      const recorder = new MediaRecorder(stream, {
        mimeType: useMime,
        videoBitsPerSecond: 2_000_000, // 2 Mbps
      });
      browserCameraMediaRecorder = recorder;

      let chunkCount = 0;
      recorder.ondataavailable = (e) => {
        if (e.data && e.data.size > 0 && ws.readyState === WebSocket.OPEN) {
          chunkCount++;
          e.data.arrayBuffer().then(buf => ws.send(buf));
        }
      };
      recorder.onerror = (e) => {
        console.error('MediaRecorder 错误:', e.error);
        showToast('编码出错，已停止推流', 'error');
      };
      // timeslice=100ms: 产出频率约 10 chunk/s，降低延迟
      recorder.start(100);
      console.log(`[H264 Camera] MediaRecorder 已启动, timeslice=100ms`);
    } catch (e) {
      console.error('MediaRecorder 创建失败:', e);
      showToast('编码启动失败: ' + e.message, 'error');
      ws.close();
    }
  };

  ws.onmessage = (evt) => {
    try {
      const msg = JSON.parse(evt.data);
      if (!msg.ok) console.warn('帧处理失败:', msg.error);
    } catch {}
  };

  ws.onerror = () => { console.warn('H264 WebSocket 错误'); };

  ws.onclose = (evt) => {
    if (!cameraTaskId) return;
    if (evt.code !== 1000) {
      showToast('摄像头连接断开，尝试重连…', 'info');
      _scheduleReconnect('h264-upload-cam', () => {
        if (!cameraTaskId) return;
        const newWs = new WebSocket(wsUrl);
        browserCameraWs = newWs;
        // 重连时重新走完整 onopen 流程（简化处理：回退 MJPEG）
        showToast('H264 重连暂不支持，已回退 MJPEG', 'info');
        setupMjpegCameraWs(wsUrl, stream);
      }, cameraTaskId);
    }
  };
}

/** WebRTC 模式：浏览器直连服务器，超低延迟推流 */
function setupWebRTCCamera(taskId, stream) {
  let pc = null;

  async function connect() {
    try {
      pc = new RTCPeerConnection({
        iceServers: [],  // 局域网无需 STUN/TURN
      });

      // 添加摄像头轨道
      stream.getTracks().forEach(track => pc.addTrack(track, stream));

      // ICE 候选收集（带超时，防止永远挂起）
      const offer = await pc.createOffer();
      await pc.setLocalDescription(offer);

      await new Promise((resolve) => {
        if (pc.iceGatheringState === 'complete') {
          resolve();
          return;
        }
        const timer = setTimeout(() => {
          // 超时也继续，用已收集的候选
          resolve();
        }, 3000);
        pc.addEventListener('icegatheringstatechange', () => {
          if (pc.iceGatheringState === 'complete') {
            clearTimeout(timer);
            resolve();
          }
        });
      });

      // 发送 offer 给服务器
      const resp = await fetch(`${PIPE_API}/webrtc/offer/${taskId}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sdp: pc.localDescription.sdp,
          type: pc.localDescription.type,
        }),
      });

      if (!resp.ok) {
        const err = await resp.json();
        throw new Error(err.detail || 'WebRTC 信令失败');
      }

      const answer = await resp.json();
      await pc.setRemoteDescription(new RTCSessionDescription(answer));

      showToast('摄像头已连接 (WebRTC)，开始推流');
      updateCameraStatus('running', 'WebRTC 推流中...');
      document.getElementById('btnStartCamera').style.display = 'none';
      document.getElementById('btnStopCamera').style.display = '';

      connectCameraH264(taskId);
      startCameraPolling();

      browserCameraTimer = true; // 标记正在推流

      pc.addEventListener('connectionstatechange', () => {
        if (pc.connectionState === 'failed') {
          showToast('WebRTC 连接失败', 'error');
        }
      });

    } catch (e) {
      console.error('WebRTC 连接失败:', e);
      showToast('WebRTC 连接失败: ' + e.message, 'error');
      if (pc) { pc.close(); pc = null; }
    }
  }

  connect();

  // 暴露给 stopCameraPipeline 使用
  browserCameraWs = { close: () => { if (pc) { pc.close(); pc = null; } } };
}

function startFrameCapture(ws, stream) {
  const video = document.getElementById('browserCameraPreview');
  if (!video) return;

  const OUT_W = 640;
  const OUT_H = 480;
  const CAPTURE_INTERVAL = Math.round(1000 / (browserCameraCaptureFps || 15));
  const JPEG_QUALITY = 0.7;

  const doCapture = () => {
    if (!browserCameraCanvas) {
      browserCameraCanvas = document.createElement('canvas');
    }
    const canvas = browserCameraCanvas;
    canvas.width = OUT_W;
    canvas.height = OUT_H;
    const ctx = canvas.getContext('2d');

    const capture = () => {
      if (ws.readyState !== WebSocket.OPEN) return;
      ctx.drawImage(video, 0, 0, OUT_W, OUT_H);

      // toBlob 异步但比 toDataURL 轻量，避免主线程 base64 编解码开销
      canvas.toBlob((blob) => {
        if (!blob || ws.readyState !== WebSocket.OPEN) return;
        const reader = new FileReader();
        reader.onload = () => {
          if (ws.readyState === WebSocket.OPEN) {
            ws.send(reader.result);
          }
        };
        reader.readAsArrayBuffer(blob);
      }, 'image/jpeg', JPEG_QUALITY);
    };

    browserCameraTimer = setInterval(capture, CAPTURE_INTERVAL);
  };

  if (video.readyState >= 2) {
    doCapture();
  } else {
    video.addEventListener('loadeddata', doCapture, { once: true });
  }
}

function stopFrameCapture() {
  _clearReconnect('mjpeg-cam');
  _clearReconnect('h264-upload-cam');
  if (browserCameraMediaRecorder && browserCameraMediaRecorder.state !== 'inactive') {
    browserCameraMediaRecorder.stop();
  }
  browserCameraMediaRecorder = null;
  if (browserCameraTimer) {
    if (typeof browserCameraTimer === 'number') {
      clearInterval(browserCameraTimer);
    }
    browserCameraTimer = null;
  }
  if (browserCameraWs) {
    browserCameraWs.close();
    browserCameraWs = null;
  }
  if (browserCameraStream) {
    browserCameraStream.getTracks().forEach(t => t.stop());
    browserCameraStream = null;
  }
  const preview = document.getElementById('browserCameraPreview');
  if (preview) {
    preview.srcObject = null;
    preview.style.display = 'none';
  }
  const placeholder = document.getElementById('browserCameraPreviewPlaceholder');
  if (placeholder) placeholder.style.display = '';
  browserCameraCanvas = null;
}

async function startCameraPipeline() {
  const input = getCameraInput();

  if (input === '__browser__') {
    await startBrowserCamera();
    return;
  }

  if (!input) { showToast('请输入摄像头地址', 'error'); return; }

  const btn = document.getElementById('btnStartCamera');
  btn.disabled = true;
  btn.innerHTML = '<span class="loading-spinner"></span> 启动中...';

  try {
    let videoFilename;
    if (input === '0') {
      videoFilename = '__camera__0';
    } else if (input.startsWith('rtsp://') || input.startsWith('rtmp://') || input.startsWith('http://')) {
      videoFilename = input;
    } else {
      videoFilename = input;
    }

    const resp = await fetch(`${PIPE_API}/start`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        video_filename: videoFilename,
        concurrent_mode: document.getElementById('camOptConcurrent').checked,
        ...collectCameraParams(),
      }),
    });
    const data = await resp.json();
    if (!resp.ok) throw new Error(data.detail || '启动失败');

    cameraTaskId = data.task_id;
    updateCameraStatus('running', '摄像头识别运行中...');
    document.getElementById('btnStartCamera').style.display = 'none';
    document.getElementById('btnStopCamera').style.display = '';
    showToast('摄像头 Pipeline 已启动');

    // H.264 WebSocket + MSE 播放
    connectCameraH264(cameraTaskId);

    startCameraPolling();
  } catch (e) {
    showToast('启动失败: ' + e.message, 'error');
  } finally {
    btn.disabled = false;
    btn.innerHTML = '▶ 启动摄像头识别';
  }
}

async function stopCameraPipeline() {
  const taskId = cameraTaskId;

  // 立即停止轮询，防止后续 pollCameraStatus 干扰新任务
  stopCameraPolling();
  cameraTaskId = null;

  stopFrameCapture();

  // 断开 H.264 推流
  disconnectCameraH264();

  updateCameraStatus('idle', '正在停止...');
  resetCameraButtons();

  if (taskId) {
    try {
      await fetch(`${PIPE_API}/stop/${taskId}`, { method: 'POST' });
    } catch {}
  }

  const cameraStream = document.getElementById('cameraStream');
  const cameraPlaceholder = document.getElementById('cameraStreamPlaceholder');
  if (cameraStream) {
    cameraStream.pause();
    cameraStream.src = '';
    cameraStream.style.display = 'none';
  }
  if (cameraPlaceholder) cameraPlaceholder.style.display = '';

  showToast('摄像头已停止');
}

function startCameraPolling() {
  stopCameraPolling();
  cameraPollTimer = setInterval(pollCameraStatus, 3000);
}

function stopCameraPolling() {
  if (cameraPollTimer) {
    clearInterval(cameraPollTimer);
    cameraPollTimer = null;
  }
}

async function pollCameraStatus() {
  // 快照当前任务 ID，防止请求返回时 cameraTaskId 已变为新任务
  const taskId = cameraTaskId;
  if (!taskId) return;
  try {
    const resp = await fetch(`${PIPE_API}/status/${taskId}`);
    if (resp.status === 404) {
      if (cameraTaskId === taskId) {
        stopCameraPolling();
        resetCameraButtons();
        cameraTaskId = null;
      }
      return;
    }
    const data = await resp.json();
    updateCameraStatus(data.status, data.progress || data.error || '');

    if (data.status !== 'running') {
      if (cameraTaskId === taskId) {
        stopCameraPolling();
        resetCameraButtons();
        disconnectCameraH264();
        if (data.status === 'completed') {
          showToast('✅ 摄像头处理完成');
        } else if (data.status === 'failed') {
          const errorMsg = data.error || '未知错误';
          if (errorMsg === '用户手动停止') {
            showToast('摄像头已停止', 'info');
          } else {
            showToast('摄像头处理失败: ' + errorMsg, 'error');
          }
        }
        cameraTaskId = null;
      }
    }
  } catch (e) {
    console.error('摄像头状态轮询失败:', e);
  }
}

function updateCameraStatus(status, text) {
  const dot = document.querySelector('#cameraStatus .status-dot');
  const statusText = document.getElementById('cameraStatusText');
  if (!dot || !statusText) return;
  dot.className = 'status-dot ' + (status === 'running' ? 'running' : status === 'completed' ? 'completed' : status === 'failed' ? 'failed' : 'idle');
  statusText.textContent = text || status;
}

function resetCameraButtons() {
  const startBtn = document.getElementById('btnStartCamera');
  const stopBtn = document.getElementById('btnStopCamera');
  if (startBtn) startBtn.style.display = '';
  if (stopBtn) stopBtn.style.display = 'none';
}

// ── 摄像头 H.264 推流状态 ──
let _camH264Ws = null;
let _camH264MediaSource = null;
let _camH264SourceBuffer = null;
let _camH264Queue = [];

function connectCameraH264(taskId) {
  disconnectCameraH264();

  const wsProto = location.protocol === 'https:' ? 'wss' : 'ws';
  const wsUrl = `${wsProto}://${location.host}${PIPE_API}/ws/h264/${taskId}`;

  const videoEl = document.getElementById('cameraStream');
  const placeholder = document.getElementById('cameraStreamPlaceholder');
  const fpsEl = document.getElementById('cameraStreamFps');
  if (!videoEl) return;

  // 显示 video，隐藏 placeholder
  videoEl.style.display = '';
  if (placeholder) placeholder.style.display = 'none';
  if (fpsEl) { fpsEl.style.display = ''; fpsEl.textContent = '连接中...'; }

  const ms = new MediaSource();
  videoEl.src = URL.createObjectURL(ms);
  videoEl.load();  // 强制加载，确保 sourceopen 触发
  _camH264MediaSource = ms;
  _camH264SourceBuffer = null;
  _camH264Queue = [];

  let _connectAttempt = 0;

  function _processCamQueue() {
    const sb = _camH264SourceBuffer;
    if (!sb || sb.updating) return;
    try {
      const vEl = document.getElementById('cameraStream');
      if (vEl && sb.buffered.length > 0 && sb.buffered.start(0) < vEl.currentTime - 8) {
        sb.remove(sb.buffered.start(0), vEl.currentTime - 3);
        return;
      }
    } catch (e) {}
    if (_camH264Queue.length > 0) {
      try { sb.appendBuffer(_camH264Queue.shift()); } catch (e) {
        if (e.name === 'QuotaExceededError') {
          _camH264Queue.length = 0;
          try {
            const vEl = document.getElementById('cameraStream');
            if (vEl && sb.buffered.length > 0) sb.remove(sb.buffered.start(0), vEl.currentTime);
          } catch (e2) {}
        }
      }
    }
  }

  function _tryConnect() {
    _connectAttempt++;
    if (_connectAttempt > 1) {
      console.log(`[H264 Result] 重试连接 #${_connectAttempt}...`);
    }
    const ws = new WebSocket(wsUrl);
    ws.binaryType = 'arraybuffer';
    _camH264Ws = ws;

    let frameCount = 0;
    let fpsTimer = performance.now();

    ws.onmessage = (evt) => {
      if (evt.data instanceof ArrayBuffer) {
        const view = new DataView(evt.data);
        const msgType = view.getUint8(0);
        const payload = evt.data.slice(5);

        if (msgType === 0x01) {
          // Init segment（仅首次创建 SourceBuffer）
          if (_camH264SourceBuffer) {
            if (!_camH264SourceBuffer.updating) {
              try { _camH264SourceBuffer.appendBuffer(payload); } catch (e) {}
            }
            return;
          }
          try {
            if (ms.readyState !== 'open') {
              console.warn('摄像头 MediaSource 未就绪，忽略 init segment');
              return;
            }
            const sb = ms.addSourceBuffer('video/mp4; codecs="avc1.42C01F"');
            _camH264SourceBuffer = sb;
            sb.addEventListener('updateend', () => { _processCamQueue(); });
            sb.appendBuffer(payload);
          } catch (e) {
            console.error('摄像头 MSE SourceBuffer 创建失败:', e);
          }
        } else if (msgType === 0x02) {
          // Media segment
          const sb = _camH264SourceBuffer;
          if (!sb) return;
          if (sb.updating) {
            if (_camH264Queue.length >= 12) _camH264Queue = _camH264Queue.slice(-6);
            _camH264Queue.push(payload);
          } else {
            try { sb.appendBuffer(payload); } catch (e) {
              if (e.name === 'QuotaExceededError') {
                try {
                  const vEl = document.getElementById('cameraStream');
                  if (vEl && sb.buffered.length > 0) sb.remove(sb.buffered.start(0), vEl.currentTime - 2);
                } catch (e2) {}
                _camH264Queue.unshift(payload);
              }
            }
          }
          frameCount++;
          const now = performance.now();
          if (now - fpsTimer > 1000) {
            const fps = (frameCount * 1000 / (now - fpsTimer)).toFixed(1);
            if (fpsEl) fpsEl.textContent = `${fps} seg/s`;
            frameCount = 0;
            fpsTimer = now;
          }
        }
      } else {
        try {
          const msg = JSON.parse(evt.data);
          if (msg.type === 'done') {
            disconnectCameraH264();
            if (fpsEl) fpsEl.textContent = '处理完成';
          }
        } catch {}
      }
    };

    ws.onclose = () => {
      if (cameraTaskId === taskId && _connectAttempt < 10) {
        const delay = Math.min(1000 * Math.pow(1.5, _connectAttempt - 1), 8000);
        if (fpsEl) fpsEl.textContent = `重连中 (${(delay/1000).toFixed(1)}s)...`;
        setTimeout(() => {
          if (cameraTaskId === taskId) _tryConnect();
        }, delay);
      }
    };
    ws.onerror = () => {};
  }

  ms.addEventListener('sourceopen', () => {
    // 首次延迟 1.5 秒再连接，给后端 ffmpeg 启动时间
    setTimeout(() => {
      if (cameraTaskId === taskId) _tryConnect();
    }, 1500);
  });
}

function disconnectCameraH264() {
  _clearReconnect('h264-cam');
  if (_camH264Ws) { _camH264Ws.onclose = null; _camH264Ws.close(); _camH264Ws = null; }
  if (_camH264MediaSource && _camH264MediaSource.readyState === 'open') {
    try { _camH264MediaSource.endOfStream(); } catch {}
  }
  _camH264MediaSource = null;
  _camH264SourceBuffer = null;
  _camH264Queue = [];
  const videoEl = document.getElementById('cameraStream');
  if (videoEl) { videoEl.pause(); videoEl.src = ''; }
  const fpsEl = document.getElementById('cameraStreamFps');
  if (fpsEl) fpsEl.textContent = '';
}

// ── WebSocket 自动重连（指数退避 + 状态检查 + 最大重试）──
const _reconnectStates = new Map(); // key → {delay, timer, retries}
const MAX_RECONNECT_RETRIES = 5;

async function _checkTaskRunning(taskId) {
  try {
    const resp = await fetch(`${PIPE_API}/status/${taskId}`);
    if (!resp.ok) return false;
    const data = await resp.json();
    return data.status === 'running';
  } catch { return false; }
}

function _scheduleReconnect(key, connectFn, taskId) {
  let state = _reconnectStates.get(key);
  if (!state) {
    state = { delay: 1000, timer: null, retries: 0 };
    _reconnectStates.set(key, state);
  }
  if (state.timer) clearTimeout(state.timer);

  if (state.retries >= MAX_RECONNECT_RETRIES) {
    _reconnectStates.delete(key);
    return;
  }
  state.retries++;

  state.timer = setTimeout(async () => {
    // 重连前检查任务是否还在运行
    if (taskId) {
      const running = await _checkTaskRunning(taskId);
      if (!running) {
        _reconnectStates.delete(key);
        return;
      }
    }
    _reconnectStates.delete(key);
    connectFn();
  }, state.delay);
  state.delay = Math.min(state.delay * 2, 16000); // 1s → 2s → 4s → ... → 16s max
}

function _clearReconnect(key) {
  const state = _reconnectStates.get(key);
  if (state) {
    clearTimeout(state.timer);
    _reconnectStates.delete(key);
  }
}

// ── 工具函数 ──
if (typeof escHtml === 'undefined') {
  function escHtml(s) {
    const d = document.createElement('div');
    d.textContent = s;
    return d.innerHTML;
  }
}
if (typeof escAttr === 'undefined') {
  function escAttr(s) {
    return s.replace(/\\/g, '\\\\').replace(/"/g, '\\"').replace(/'/g, "\\'");
  }
}

/** 安全地将文件名插入 HTML 属性（防 XSS） */
function safeAttr(s) {
  return s.replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/'/g, '&#39;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}
