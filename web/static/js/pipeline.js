/**
 * Pipeline 前端逻辑 — 视频 Demo / 摄像头 Demo（修复版）
 *
 * 修复内容：
 * 1. selectVideo 的 event 未传入问题
 * 2. 摄像头输入不经过 _safe_filename 和文件存在检查
 * 3. 摄像头实时流显示（MJPEG）
 * 4. Pipeline 处理期间实时进度显示
 * 5. Tab 切换时正确初始化
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
    onCameraSourceChange(); // 初始化摄像头输入框状态
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
           onclick="selectVideo('${escAttr(v.filename)}', this)">
        <div class="video-item-icon">🎬</div>
        <div class="video-item-info">
          <div class="video-item-name">${escHtml(v.filename)}</div>
          <div class="video-item-meta">${v.size_mb} MB</div>
        </div>
        <div class="video-item-actions">
          <button class="btn btn-outline btn-sm" onclick="event.stopPropagation(); playSourceVideo('${escAttr(v.filename)}')">▶ 预览</button>
          <button class="btn btn-danger btn-sm" onclick="event.stopPropagation(); deleteVideo('${escAttr(v.filename)}')">🗑️</button>
        </div>
      </div>
    `).join('');
  } catch (e) {
    container.innerHTML = `<div class="empty-msg">加载失败: ${e.message}</div>`;
  }
}

// 修复：接收 event.currentTarget 作为参数
function selectVideo(filename, el) {
  selectedVideo = filename;
  document.getElementById('pipelineControl').style.display = '';
  // 更新选中状态
  document.querySelectorAll('.video-item').forEach(item => item.classList.remove('selected'));
  if (el) el.classList.add('selected');
  // 加载源视频
  playSourceVideo(filename);
  // 重置结果
  document.getElementById('resultVideo').style.display = 'none';
  document.getElementById('resultPlaceholder').style.display = '';
  resetPipelineStatus();
}

function playSourceVideo(filename) {
  const video = document.getElementById('sourceVideo');
  if (!video) return;
  video.src = `${PIPE_API}/video/${encodeURIComponent(filename)}`;
  video.load();
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
        use_agent: document.getElementById('optAgent').checked,
        concurrent_mode: document.getElementById('optConcurrent').checked,
      }),
    });
    const data = await resp.json();
    if (!resp.ok) throw new Error(data.detail || '启动失败');

    currentTaskId = data.task_id;
    showToast(`Pipeline 已启动 (${currentTaskId})`);
    updatePipelineStatus('running', '处理中...');
    document.getElementById('btnStartPipeline').style.display = 'none';
    document.getElementById('btnStopPipeline').style.display = '';

    // 实时预览：显示 MJPEG 流
    const resultVideo = document.getElementById('resultVideo');
    const resultPlaceholder = document.getElementById('resultPlaceholder');
    if (resultVideo) {
      resultVideo.style.display = 'none';
    }
    // 复用 resultPlaceholder 区域显示实时流
    if (resultPlaceholder) {
      resultPlaceholder.innerHTML = `<img id="livePreview" src="${PIPE_API}/stream/${currentTaskId}" style="max-width:100%;border-radius:8px;background:#000" alt="实时预览" />`;
      resultPlaceholder.style.display = '';
    }

    startStatusPolling();
  } catch (e) {
    showToast('启动失败: ' + e.message, 'error');
  } finally {
    btn.disabled = false;
    btn.innerHTML = '▶ 开始处理';
  }
}

async function stopVideoPipeline() {
  if (!currentTaskId) return;
  try {
    const resp = await fetch(`${PIPE_API}/stop/${currentTaskId}`, { method: 'POST' });
    const data = await resp.json();
    if (!resp.ok) throw new Error(data.detail || '停止失败');
    showToast('已停止');
    updatePipelineStatus('failed', '已停止');
    // 清除实时预览
    const resultPlaceholder = document.getElementById('resultPlaceholder');
    if (resultPlaceholder) {
      resultPlaceholder.innerHTML = '<span>🎬</span><p>处理完成后在此播放结果</p>';
    }
    resetPipelineButtons();
    stopStatusPolling();
  } catch (e) {
    showToast(e.message, 'error');
  }
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
    const data = await resp.json();
    updatePipelineStatus(data.status, data.progress || data.error || '');

    if (data.status === 'completed') {
      stopStatusPolling();
      resetPipelineButtons();
      showToast('✅ 处理完成!');
      // 清除实时预览
      const resultPlaceholder = document.getElementById('resultPlaceholder');
      if (resultPlaceholder) {
        resultPlaceholder.innerHTML = '<span>🎬</span><p>处理完成后在此播放结果</p>';
      }
      if (data.output_filename) {
        const resultVideo = document.getElementById('resultVideo');
        resultVideo.src = `${PIPE_API}/outputs/${encodeURIComponent(data.output_filename)}`;
        resultVideo.style.display = '';
        if (resultPlaceholder) resultPlaceholder.style.display = 'none';
        resultVideo.load();
      }
      loadTaskHistory();
    } else if (data.status === 'failed') {
      stopStatusPolling();
      resetPipelineButtons();
      // 清除实时预览
      const resultPlaceholder = document.getElementById('resultPlaceholder');
      if (resultPlaceholder) {
        resultPlaceholder.innerHTML = '<span>🎬</span><p>处理完成后在此播放结果</p>';
      }
      showToast('处理失败: ' + (data.error || '未知错误'), 'error');
      loadTaskHistory();
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
              任务 ${t.task_id} · ${t.progress || t.error || t.status}
              ${t.output_filename ? ' · 输出: ' + escHtml(t.output_filename) : ''}
            </div>
          </div>
          <div class="task-actions">
            ${t.status === 'completed' && t.output_filename ? `<button class="btn btn-outline btn-sm" onclick="playResultVideo('${escAttr(t.output_filename)}')">▶ 播放</button>` : ''}
            ${t.status === 'running' ? `<button class="btn btn-danger btn-sm" onclick="stopTaskById('${escAttr(t.task_id)}')">⏹ 停止</button>` : ''}
          </div>
        </div>
      `;
    }).join('');
  } catch (e) {
    container.innerHTML = `<div class="empty-msg">加载失败: ${e.message}</div>`;
  }
}

function playResultVideo(filename) {
  switchTab('video-demo');
  const resultVideo = document.getElementById('resultVideo');
  if (!resultVideo) return;
  resultVideo.src = `${PIPE_API}/outputs/${encodeURIComponent(filename)}`;
  resultVideo.style.display = '';
  document.getElementById('resultPlaceholder').style.display = 'none';
  resultVideo.load();
  document.getElementById('pipelineControl').style.display = '';
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

  // 显示/隐藏浏览器预览
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
    // 1. 获取浏览器摄像头（需要 HTTPS 或 localhost）
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

    // 显示本地预览
    const preview = document.getElementById('browserCameraPreview');
    const placeholder = document.getElementById('browserCameraPreviewPlaceholder');
    if (preview) {
      preview.srcObject = stream;
      preview.style.display = '';
    }
    if (placeholder) placeholder.style.display = 'none';

    // 2. 启动后端 Pipeline
    const resp = await fetch(`${PIPE_API}/start-browser-camera`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        use_agent: document.getElementById('camOptAgent').checked,
        concurrent_mode: document.getElementById('camOptConcurrent').checked,
      }),
    });
    const data = await resp.json();
    if (!resp.ok) throw new Error(data.detail || '启动失败');

    cameraTaskId = data.task_id;

    // 3. 建立 WebSocket 推流
    const wsProto = location.protocol === 'https:' ? 'wss' : 'ws';
    const wsUrl = `${wsProto}://${location.host}${PIPE_API}/ws/camera/${cameraTaskId}`;
    const ws = new WebSocket(wsUrl);
    browserCameraWs = ws;

    ws.onopen = () => {
      showToast('摄像头已连接，开始推流');
      document.getElementById('cameraStatusCard').style.display = '';
      updateCameraStatus('running', '浏览器摄像头推流中...');
      document.getElementById('btnStartCamera').style.display = 'none';
      document.getElementById('btnStopCamera').style.display = '';

      // 设置 MJPEG 实时流画面
      const cameraStream = document.getElementById('cameraStream');
      const cameraPlaceholder = document.getElementById('cameraStreamPlaceholder');
      if (cameraStream) {
        cameraStream.src = `${PIPE_API}/stream/${cameraTaskId}`;
        cameraStream.style.display = '';
        if (cameraPlaceholder) cameraPlaceholder.style.display = 'none';
      }

      // 开始捕获帧
      startFrameCapture(ws, stream);
      startCameraPolling();
    };

    ws.onmessage = (evt) => {
      try {
        const msg = JSON.parse(evt.data);
        if (!msg.ok) console.warn('帧发送失败:', msg.error);
      } catch {}
    };

    ws.onerror = () => {
      showToast('WebSocket 连接错误', 'error');
      stopBrowserCamera();
    };

    ws.onclose = () => {
      if (cameraTaskId) {
        showToast('WebSocket 已断开');
        stopBrowserCamera();
      }
    };

  } catch (e) {
    showToast('启动失败: ' + e.message, 'error');
    stopBrowserCamera();
  } finally {
    btn.disabled = false;
    btn.innerHTML = '▶ 启动摄像头识别';
  }
}

function startFrameCapture(ws, stream) {
  // 创建离屏 canvas
  const video = document.getElementById('browserCameraPreview');
  if (!video) return;

  // 等视频就绪
  const doCapture = () => {
    if (!browserCameraCanvas) {
      browserCameraCanvas = document.createElement('canvas');
    }
    const canvas = browserCameraCanvas;
    const targetW = 1280;
    const targetH = 720;

    browserCameraTimer = setInterval(() => {
      if (ws.readyState !== WebSocket.OPEN) return;

      // 从 video 元素捕获帧
      canvas.width = video.videoWidth || targetW;
      canvas.height = video.videoHeight || targetH;
      const ctx = canvas.getContext('2d');
      ctx.drawImage(video, 0, 0, canvas.width, canvas.height);

      canvas.toBlob((blob) => {
        if (!blob || ws.readyState !== WebSocket.OPEN) return;
        ws.send(blob);
      }, 'image/jpeg', 0.7);
    }, 66); // ~15fps
  };

  if (video.readyState >= 2) {
    doCapture();
  } else {
    video.addEventListener('loadeddata', doCapture, { once: true });
  }
}

function stopFrameCapture() {
  if (browserCameraTimer) {
    clearInterval(browserCameraTimer);
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

  // 浏览器摄像头走独立流程
  if (input === '__browser__') {
    await startBrowserCamera();
    return;
  }

  if (!input) { showToast('请输入摄像头地址', 'error'); return; }

  const btn = document.getElementById('btnStartCamera');
  btn.disabled = true;
  btn.innerHTML = '<span class="loading-spinner"></span> 启动中...';

  try {
    // 构造摄像头标识符
    let videoFilename;
    if (input === '0') {
      videoFilename = '__camera__0';
    } else if (input.startsWith('rtsp://') || input.startsWith('rtmp://') || input.startsWith('http://')) {
      videoFilename = input;  // 直接传 URL，后端识别
    } else {
      videoFilename = input;  // 自定义路径
    }

    const resp = await fetch(`${PIPE_API}/start`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        video_filename: videoFilename,
        use_agent: document.getElementById('camOptAgent').checked,
        concurrent_mode: document.getElementById('camOptConcurrent').checked,
        display: document.getElementById('camOptDisplay').checked,
      }),
    });
    const data = await resp.json();
    if (!resp.ok) throw new Error(data.detail || '启动失败');

    cameraTaskId = data.task_id;
    document.getElementById('cameraStatusCard').style.display = '';
    updateCameraStatus('running', '摄像头识别运行中...');
    document.getElementById('btnStartCamera').style.display = 'none';
    document.getElementById('btnStopCamera').style.display = '';
    showToast('摄像头 Pipeline 已启动');

    // 设置 MJPEG 实时流画面
    const cameraStream = document.getElementById('cameraStream');
    const cameraPlaceholder = document.getElementById('cameraStreamPlaceholder');
    if (cameraStream) {
      cameraStream.src = `${PIPE_API}/stream/${cameraTaskId}`;
      cameraStream.style.display = '';
      if (cameraPlaceholder) cameraPlaceholder.style.display = 'none';
    }

    startCameraPolling();
  } catch (e) {
    showToast('启动失败: ' + e.message, 'error');
  } finally {
    btn.disabled = false;
    btn.innerHTML = '▶ 启动摄像头识别';
  }
}

async function stopCameraPipeline() {
  // 停止浏览器摄像头推流
  stopFrameCapture();

  if (cameraTaskId) {
    try {
      await fetch(`${PIPE_API}/stop/${cameraTaskId}`, { method: 'POST' });
    } catch {}
  }
  updateCameraStatus('idle', '已停止');
  resetCameraButtons();
  stopCameraPolling();

  // 清除实时流
  const cameraStream = document.getElementById('cameraStream');
  const cameraPlaceholder = document.getElementById('cameraStreamPlaceholder');
  if (cameraStream) {
    cameraStream.src = '';
    cameraStream.style.display = 'none';
    if (cameraPlaceholder) cameraPlaceholder.style.display = '';
  }

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
    const data = await resp.json();
    updateCameraStatus(data.status, data.progress || data.error || '');

    if (data.status !== 'running') {
      stopCameraPolling();
      resetCameraButtons();
      if (data.status === 'completed') {
        showToast('✅ 摄像头处理完成');
      } else if (data.status === 'failed') {
        showToast('摄像头处理失败: ' + (data.error || ''), 'error');
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
