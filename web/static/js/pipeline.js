/**
 * Pipeline 前端逻辑 — 视频 Demo / 摄像头 Demo
 */

const PIPE_API = '/api/pipeline';

// ── Tab 切换 ──
function switchTab(tabName) {
  // 按钮状态
  document.querySelectorAll('.tab-btn').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.tab === tabName);
  });
  // 内容显示
  document.querySelectorAll('.tab-content').forEach(el => {
    el.classList.toggle('active', el.id === `tab-${tabName}`);
  });
  // 切换到视频 tab 时加载列表
  if (tabName === 'video-demo') {
    loadVideoList();
    loadTaskHistory();
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

videoFileInput.addEventListener('change', function (e) {
  if (e.target.files.length > 0) handleVideoUpload(e.target.files[0]);
});

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

async function handleVideoUpload(file) {
  // 验证
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

    // 使用 XMLHttpRequest 获取上传进度
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

  // 清空 input
  videoFileInput.value = '';
}

// ── 视频列表 ──
async function loadVideoList() {
  const container = document.getElementById('videoList');
  try {
    const resp = await fetch(`${PIPE_API}/videos`);
    const data = await resp.json();
    if (!data.videos.length) {
      container.innerHTML = '<div class="empty-msg">暂无视频，请上传</div>';
      return;
    }
    container.innerHTML = data.videos.map(v => `
      <div class="video-item ${selectedVideo === v.filename ? 'selected' : ''}" onclick="selectVideo('${escAttr(v.filename)}')">
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

function selectVideo(filename) {
  selectedVideo = filename;
  document.getElementById('pipelineControl').style.display = '';
  // 更新选中状态
  document.querySelectorAll('.video-item').forEach(el => el.classList.remove('selected'));
  event.currentTarget.classList.add('selected');
  // 加载源视频
  playSourceVideo(filename);
  // 重置结果
  document.getElementById('resultVideo').style.display = 'none';
  document.getElementById('resultPlaceholder').style.display = '';
  resetPipelineStatus();
}

function playSourceVideo(filename) {
  const video = document.getElementById('sourceVideo');
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

    // 开始轮询状态
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
      // 加载结果视频
      if (data.output_filename) {
        const resultVideo = document.getElementById('resultVideo');
        resultVideo.src = `${PIPE_API}/outputs/${encodeURIComponent(data.output_filename)}`;
        resultVideo.style.display = '';
        document.getElementById('resultPlaceholder').style.display = 'none';
        resultVideo.load();
      }
      loadTaskHistory();
    } else if (data.status === 'failed') {
      stopStatusPolling();
      resetPipelineButtons();
      showToast('处理失败: ' + (data.error || '未知错误'), 'error');
      loadTaskHistory();
    }
  } catch (e) {
    // 静默处理轮询错误
    console.error('状态轮询失败:', e);
  }
}

function updatePipelineStatus(status, text) {
  const dot = document.querySelector('#pipelineStatus .status-dot');
  const statusText = document.getElementById('pipelineStatusText');
  dot.className = 'status-dot ' + (status === 'running' ? 'running' : status === 'completed' ? 'completed' : 'idle');
  statusText.textContent = text || status;
}

function resetPipelineStatus() {
  updatePipelineStatus('idle', '等待开始');
  resetPipelineButtons();
}

function resetPipelineButtons() {
  document.getElementById('btnStartPipeline').style.display = '';
  document.getElementById('btnStopPipeline').style.display = 'none';
}

// ── 任务历史 ──
async function loadTaskHistory() {
  const container = document.getElementById('taskHistory');
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
      return `
        <div class="task-item ${statusClass}">
          <div class="task-icon">${statusIcon}</div>
          <div class="task-info">
            <div class="task-name">${escHtml(t.video_filename)}</div>
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

function onCameraSourceChange() {
  const sel = document.getElementById('cameraSource').value;
  const urlInput = document.getElementById('cameraUrl');
  urlInput.style.display = sel === '0' ? 'none' : '';
  if (sel === 'rtsp') {
    urlInput.placeholder = 'rtsp://192.168.1.100/stream';
  } else if (sel === 'custom') {
    urlInput.placeholder = '输入视频路径或 URL';
  }
}

function getCameraInput() {
  const sel = document.getElementById('cameraSource').value;
  if (sel === '0') return '0';
  return document.getElementById('cameraUrl').value.trim();
}

async function startCameraPipeline() {
  const input = getCameraInput();
  if (!input) { showToast('请输入摄像头地址', 'error'); return; }

  const btn = document.getElementById('btnStartCamera');
  btn.disabled = true;
  btn.innerHTML = '<span class="loading-spinner"></span> 启动中...';

  try {
    // 对于摄像头，我们用特殊文件名标识
    const resp = await fetch(`${PIPE_API}/start`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        video_filename: input === '0' ? '__camera__0' : input,
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

    startCameraPolling();
  } catch (e) {
    showToast('启动失败: ' + e.message, 'error');
  } finally {
    btn.disabled = false;
    btn.innerHTML = '▶ 启动摄像头识别';
  }
}

async function stopCameraPipeline() {
  if (!cameraTaskId) return;
  try {
    await fetch(`${PIPE_API}/stop/${cameraTaskId}`, { method: 'POST' });
    updateCameraStatus('idle', '已停止');
    resetCameraButtons();
    stopCameraPolling();
    showToast('摄像头已停止');
  } catch (e) {
    showToast(e.message, 'error');
  }
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
      }
    }
  } catch {}
}

function updateCameraStatus(status, text) {
  const dot = document.querySelector('#cameraStatus .status-dot');
  const statusText = document.getElementById('cameraStatusText');
  dot.className = 'status-dot ' + (status === 'running' ? 'running' : status === 'completed' ? 'completed' : 'idle');
  statusText.textContent = text || status;
}

function resetCameraButtons() {
  document.getElementById('btnStartCamera').style.display = '';
  document.getElementById('btnStopCamera').style.display = 'none';
}

// ── 工具函数 (复用 app.js 中的) ──
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
