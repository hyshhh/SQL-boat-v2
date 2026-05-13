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
  selectedVideo = filename;
  document.getElementById('pipelineControl').style.display = '';
  // 更新选中状态
  document.querySelectorAll('.video-item').forEach(item => item.classList.remove('selected'));
  if (el) el.classList.add('selected');

  // 重置结果区域
  const resultPlaceholder = document.getElementById('resultPlaceholder');
  if (resultPlaceholder) {
    resultPlaceholder.innerHTML = '<span>🎬</span><p>点击"开始处理"后实时显示识别结果</p>';
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
    max_frames: parseInt(document.getElementById('camMaxFrames').value, 10) || 0,
    device: document.getElementById('camDevice').value,
    yolo_model: document.getElementById('camYoloModel').value.trim(),
    prompt_mode: document.getElementById('camPromptMode').value,
    enable_refresh: document.getElementById('camEnableRefresh').checked,
    gap_num: parseInt(document.getElementById('camGapNum').value, 10) || 150,
    max_concurrent: parseInt(document.getElementById('camMaxConcurrent').value, 10) || 4,
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
        <video id="streamVideo" autoplay muted playsinline style="max-width:100%;border-radius:8px;background:#000;display:block"></video>
        <div id="streamFps" style="text-align:center;font-size:12px;color:#888;margin-top:4px">连接中...</div>
      `;
      resultPlaceholder.style.display = '';
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
    let decodedFrames = 0;  // 实际解码的视频帧数

    // 用 requestVideoFrameCallback 统计真实视频帧数
    function setupFrameCounter() {
      const vEl = document.getElementById('streamVideo');
      if (vEl && 'requestVideoFrameCallback' in vEl) {
        const onFrame = () => {
          decodedFrames++;
          vEl.requestVideoFrameCallback(onFrame);
        };
        vEl.requestVideoFrameCallback(onFrame);
      }
    }

    ws.onmessage = (evt) => {
      if (evt.data instanceof ArrayBuffer) {
        const view = new DataView(evt.data);
        const msgType = view.getUint8(0);
        const payload = evt.data.slice(5);

        if (msgType === 0x01) {
          // Init segment (moov) — 创建 SourceBuffer
          try {
            const codecs = 'avc1.42C01F'; // H.264 Constrained Baseline Level 3.1
            const sb = ms.addSourceBuffer(`video/mp4; codecs="${codecs}"`);
            _h264SourceBuffer = sb;

            sb.addEventListener('updateend', () => {
              // 主动清理已播放的缓冲区，防止内存膨胀
              try {
                const videoEl = document.getElementById('streamVideo');
                if (videoEl && sb.buffered.length > 0) {
                  const start = sb.buffered.start(0);
                  const end = sb.buffered.end(0);
                  const currentTime = videoEl.currentTime;
                  // 保留当前播放位置前 5 秒 + 后面所有数据
                  if (currentTime - start > 10) {
                    sb.remove(start, Math.max(start, currentTime - 5));
                    return; // remove 会再次触发 updateend，届时再处理队列
                  }
                }
              } catch (e) {}
              // 处理队列中积压的数据
              if (_h264Queue.length > 0 && !sb.updating) {
                try {
                  sb.appendBuffer(_h264Queue.shift());
                } catch (e) {
                  if (e.name === 'QuotaExceededError') {
                    // 缓冲区满，丢弃队列中所有旧数据，只保留最新的
                    _h264Queue.length = 0;
                  }
                }
              }
            });

            sb.appendBuffer(payload);
            setupFrameCounter();  // 视频开始播放后启动帧计数
          } catch (e) {
            console.error('MSE SourceBuffer 创建失败:', e);
          }

        } else if (msgType === 0x02) {
          // Media segment (moof+mdat)
          if (_h264SourceBuffer) {
            if (_h264SourceBuffer.updating) {
              if (_h264Queue.length >= 10) {
                // 队列满了，丢掉旧帧只保留最新的
                _h264Queue.length = 0;
              }
              _h264Queue.push(payload);
            } else {
              try {
                _h264SourceBuffer.appendBuffer(payload);
              } catch (e) {
                if (e.name === 'QuotaExceededError') {
                  // 缓冲区满，尝试清理后重试
                  try {
                    const videoEl = document.getElementById('streamVideo');
                    const sb = _h264SourceBuffer;
                    if (videoEl && sb.buffered.length > 0) {
                      sb.remove(sb.buffered.start(0), Math.max(sb.buffered.start(0), videoEl.currentTime - 2));
                    }
                  } catch (e2) {}
                }
              }
            }
          }

          // FPS 统计
          frameCount++;
          const now = performance.now();
          if (now - fpsTimer > 1000) {
            const segFps = (frameCount * 1000 / (now - fpsTimer)).toFixed(1);
            const fpsEl = document.getElementById('streamFps');
            if (fpsEl) fpsEl.textContent = `${segFps} seg/s | ${decodedFrames} 帧`;
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
        setTimeout(() => {
          if (currentTaskId === taskId) connectStreamWs(taskId);
        }, 1000);
      }
    };

    ws.onerror = () => {};
  });
}

/** 断开 H.264 推流 */
function disconnectStreamWs() {
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
  const resultPlaceholder = document.getElementById('resultPlaceholder');
  if (resultPlaceholder) {
    resultPlaceholder.innerHTML = '<span>🎬</span><p>点击"开始处理"后实时显示识别结果</p>';
  }

  stopStatusPolling();
  currentTaskId = null;
  loadTaskHistory();
}

function startStatusPolling() {
  stopStatusPolling();
  statusPollTimer = setInterval(pollTaskStatus, 2000);
}

function stopStatusPolling() {
  if (statusPollTimer) {
    clearInterval(statusPollTimer);
    statusPollTimer = null;
  }
}

async function pollTaskStatus() {
  if (!currentTaskId) return;
  try {
    const resp = await fetch(`${PIPE_API}/status/${currentTaskId}`);
    if (resp.status === 404) {
      stopStatusPolling();
      resetPipelineButtons();
      currentTaskId = null;
      return;
    }
    const data = await resp.json();
    updatePipelineStatus(data.status, data.progress || data.error || '');

    if (data.status === 'completed') {
      stopStatusPolling();
      resetPipelineButtons();
      disconnectStreamWs();
      showToast('✅ 处理完成!');
      const resultPlaceholder = document.getElementById('resultPlaceholder');
      if (resultPlaceholder) {
        resultPlaceholder.innerHTML = '<span>✅</span><p>处理完成</p>';
      }
      loadTaskHistory();
      currentTaskId = null;
    } else if (data.status === 'failed') {
      stopStatusPolling();
      resetPipelineButtons();
      disconnectStreamWs();
      const resultPlaceholder = document.getElementById('resultPlaceholder');
      if (resultPlaceholder) {
        resultPlaceholder.innerHTML = '<span>🎬</span><p>点击"开始处理"后实时显示识别结果</p>';
      }
      const errorMsg = data.error || '未知错误';
      if (errorMsg === '用户手动停止') {
        showToast('已停止', 'info');
      } else {
        showToast('处理失败: ' + errorMsg, 'error');
      }
      loadTaskHistory();
      currentTaskId = null;
    }
  } catch (e) {
    console.error('状态轮询失败:', e);
  }
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

function onCameraSourceChange() {
  const sel = document.getElementById('cameraSource');
  if (!sel) return;
  const val = sel.value;
  const urlInput = document.getElementById('cameraUrl');
  const previewRow = document.getElementById('browserCameraPreviewRow');

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

    const resp = await fetch(`${PIPE_API}/start-browser-camera`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        concurrent_mode: document.getElementById('camOptConcurrent').checked,
        ...collectCameraParams(),
      }),
    });
    const data = await resp.json();
    if (!resp.ok) throw new Error(data.detail || '启动失败');

    cameraTaskId = data.task_id;

    const wsProto = location.protocol === 'https:' ? 'wss' : 'ws';
    const wsUrl = `${wsProto}://${location.host}${PIPE_API}/ws/camera/${cameraTaskId}`;

    function setupWsHandlers(ws) {
      ws.onopen = () => {
        showToast('摄像头已连接，开始推流');
        updateCameraStatus('running', '浏览器摄像头推流中...');
        document.getElementById('btnStartCamera').style.display = 'none';
        document.getElementById('btnStopCamera').style.display = '';

        // H.264 WebSocket + MSE 播放（和视频 Demo 一样）
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

      ws.onerror = () => { console.warn('WebSocket 错误'); };

      ws.onclose = (evt) => {
        if (!cameraTaskId) return;
        if (evt.code !== 1000 && cameraTaskId) {
          showToast('摄像头连接断开，尝试重连…', 'info');
          setTimeout(() => {
            if (!cameraTaskId) return;
            const newWs = new WebSocket(wsUrl);
            browserCameraWs = newWs;
            setupWsHandlers(newWs);
          }, 2000);
        }
      };
    }

    const ws = new WebSocket(wsUrl);
    browserCameraWs = ws;
    setupWsHandlers(ws);

  } catch (e) {
    showToast('启动失败: ' + e.message, 'error');
    stopBrowserCamera();
  } finally {
    btn.disabled = false;
    btn.innerHTML = '▶ 启动摄像头识别';
  }
}

function startFrameCapture(ws, stream) {
  const video = document.getElementById('browserCameraPreview');
  if (!video) return;

  // 输出尺寸与 pipeline 一致，避免无谓的高分辨率编码/传输
  const OUT_W = 640;
  const OUT_H = 480;
  const TARGET_INTERVAL = 66; // ~15fps
  const JPEG_QUALITY = 0.7;

  const doCapture = () => {
    if (!browserCameraCanvas) {
      browserCameraCanvas = document.createElement('canvas');
    }
    const canvas = browserCameraCanvas;
    canvas.width = OUT_W;
    canvas.height = OUT_H;
    const ctx = canvas.getContext('2d');

    let lastTime = 0;

    const tick = (now) => {
      browserCameraTimer = requestAnimationFrame(tick);
      if (now - lastTime < TARGET_INTERVAL) return;
      if (ws.readyState !== WebSocket.OPEN) return;
      lastTime = now;

      ctx.drawImage(video, 0, 0, OUT_W, OUT_H);

      // toDataURL 同步完成，无回调延迟，不会跳帧
      const dataUrl = canvas.toDataURL('image/jpeg', JPEG_QUALITY);
      const binary = atob(dataUrl.split(',')[1]);
      const bytes = new Uint8Array(binary.length);
      for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
      ws.send(bytes.buffer);
    };

    browserCameraTimer = requestAnimationFrame(tick);
  };

  if (video.readyState >= 2) {
    doCapture();
  } else {
    video.addEventListener('loadeddata', doCapture, { once: true });
  }
}

function stopFrameCapture() {
  if (browserCameraTimer) {
    cancelAnimationFrame(browserCameraTimer);
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
  stopFrameCapture();

  const taskId = cameraTaskId;

  // 断开 H.264 推流
  disconnectCameraH264();

  stopCameraPolling();
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

  cameraTaskId = null;
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
  if (!cameraTaskId) return;
  try {
    const resp = await fetch(`${PIPE_API}/status/${cameraTaskId}`);
    if (resp.status === 404) {
      stopCameraPolling();
      resetCameraButtons();
      cameraTaskId = null;
      return;
    }
    const data = await resp.json();
    updateCameraStatus(data.status, data.progress || data.error || '');

    if (data.status !== 'running') {
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
  _camH264MediaSource = ms;
  _camH264SourceBuffer = null;
  _camH264Queue = [];

  ms.addEventListener('sourceopen', () => {
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
          // Init segment
          try {
            const sb = ms.addSourceBuffer('video/mp4; codecs="avc1.42C01F"');
            _camH264SourceBuffer = sb;
            sb.addEventListener('updateend', () => {
              // 主动清理已播放的缓冲区
              try {
                const videoEl = document.getElementById('cameraStream');
                if (videoEl && sb.buffered.length > 0) {
                  const start = sb.buffered.start(0);
                  const currentTime = videoEl.currentTime;
                  if (currentTime - start > 10) {
                    sb.remove(start, Math.max(start, currentTime - 5));
                    return;
                  }
                }
              } catch (e) {}
              if (_camH264Queue.length > 0 && !sb.updating) {
                try { sb.appendBuffer(_camH264Queue.shift()); } catch (e) {
                  if (e.name === 'QuotaExceededError') _camH264Queue.length = 0;
                }
              }
            });
            sb.appendBuffer(payload);
          } catch (e) {
            console.error('摄像头 MSE SourceBuffer 创建失败:', e);
          }
        } else if (msgType === 0x02) {
          // Media segment
          if (_camH264SourceBuffer) {
            if (_camH264SourceBuffer.updating) {
              if (_camH264Queue.length >= 10) _camH264Queue.length = 0;
              _camH264Queue.push(payload);
            } else {
              try { _camH264SourceBuffer.appendBuffer(payload); } catch (e) {
                if (e.name === 'QuotaExceededError') {
                  try {
                    const videoEl = document.getElementById('cameraStream');
                    const sb = _camH264SourceBuffer;
                    if (videoEl && sb.buffered.length > 0) {
                      sb.remove(sb.buffered.start(0), Math.max(sb.buffered.start(0), videoEl.currentTime - 2));
                    }
                  } catch (e2) {}
                }
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
      if (cameraTaskId === taskId) {
        setTimeout(() => { if (cameraTaskId === taskId) connectCameraH264(taskId); }, 1000);
      }
    };
    ws.onerror = () => {};
  });
}

function disconnectCameraH264() {
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
