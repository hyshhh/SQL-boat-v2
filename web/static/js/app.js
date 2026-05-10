/**
 * 船只舷号管理系统 — 前端逻辑
 */

const API = '/api/ships';
let allShips = [];
let editingMode = false;

// ── Toast ──
function showToast(msg, type = 'success') {
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.className = 'toast ' + type + ' show';
  clearTimeout(t._timer);
  t._timer = setTimeout(() => t.classList.remove('show'), 3000);
}

// ── API 调用 ──
async function apiFetch(url, options = {}) {
  const resp = await fetch(url, {
    headers: { 'Content-Type': 'application/json' },
    ...options,
  });
  const data = await resp.json();
  if (!resp.ok) throw new Error(data.detail || '请求失败');
  return data;
}

// ── 加载数据 ──
async function loadShips() {
  try {
    const [shipsData, statsData] = await Promise.all([
      apiFetch(API),
      apiFetch(`${API}/stats`),
    ]);
    allShips = shipsData.ships;
    document.getElementById('totalCount').textContent = statsData.total_ships;
    document.getElementById('backendType').textContent = statsData.backend.toUpperCase();
    renderTable(allShips);
  } catch (e) {
    showToast('加载失败: ' + e.message, 'error');
  }
}

// ── 渲染表格 ──
function renderTable(ships) {
  const tbody = document.getElementById('shipTable');
  if (!ships.length) {
    tbody.innerHTML = '<tr><td colspan="3" class="empty-msg">暂无数据</td></tr>';
    return;
  }
  tbody.innerHTML = ships.map(s => `
    <tr>
      <td><span class="hull-num">${escHtml(s.hull_number)}</span></td>
      <td>${escHtml(s.description)}</td>
      <td>
        <div class="actions">
          <button class="btn btn-outline btn-sm" onclick='openEditModal("${escAttr(s.hull_number)}","${escAttr(s.description)}")'>编辑</button>
          <button class="btn btn-danger btn-sm" onclick='deleteShip("${escAttr(s.hull_number)}")'>删除</button>
        </div>
      </td>
    </tr>
  `).join('');
}

// ── 搜索 ──
document.getElementById('searchInput').addEventListener('input', function () {
  const q = this.value.toLowerCase().trim();
  if (!q) { renderTable(allShips); return; }
  const filtered = allShips.filter(s =>
    s.hull_number.toLowerCase().includes(q) ||
    s.description.toLowerCase().includes(q)
  );
  renderTable(filtered);
});

// ── 新增弹窗 ──
function openAddModal() {
  editingMode = false;
  document.getElementById('modalTitle').textContent = '新增船只';
  document.getElementById('modalHullNumber').value = '';
  document.getElementById('modalHullNumber').disabled = false;
  document.getElementById('modalDescription').value = '';
  document.getElementById('shipModal').classList.add('active');
}

// ── 编辑弹窗 ──
function openEditModal(hn, desc) {
  editingMode = true;
  document.getElementById('modalTitle').textContent = '编辑船只';
  document.getElementById('modalHullNumber').value = hn;
  document.getElementById('modalHullNumber').disabled = true;
  document.getElementById('modalDescription').value = desc;
  document.getElementById('shipModal').classList.add('active');
}

function closeModal() {
  document.getElementById('shipModal').classList.remove('active');
}

// ── 提交新增/编辑 ──
async function submitShip() {
  const hn = document.getElementById('modalHullNumber').value.trim();
  const desc = document.getElementById('modalDescription').value.trim();
  if (!hn || !desc) { showToast('舷号和描述不能为空', 'error'); return; }
  try {
    if (editingMode) {
      await apiFetch(`${API}/${encodeURIComponent(hn)}`, {
        method: 'PUT', body: JSON.stringify({ description: desc }),
      });
      showToast('更新成功');
    } else {
      await apiFetch(API, {
        method: 'POST', body: JSON.stringify({ hull_number: hn, description: desc }),
      });
      showToast('添加成功');
    }
    closeModal();
    loadShips();
  } catch (e) {
    showToast(e.message, 'error');
  }
}

// ── 删除 ──
async function deleteShip(hn) {
  if (!confirm(`确定删除弦号 ${hn}？`)) return;
  try {
    await apiFetch(`${API}/${encodeURIComponent(hn)}`, { method: 'DELETE' });
    showToast('删除成功');
    loadShips();
  } catch (e) {
    showToast(e.message, 'error');
  }
}

// ── 批量导入 ──
function openBulkModal() {
  document.getElementById('bulkInput').value = '';
  document.getElementById('bulkModal').classList.add('active');
}
function closeBulkModal() {
  document.getElementById('bulkModal').classList.remove('active');
}
async function submitBulk() {
  const raw = document.getElementById('bulkInput').value.trim();
  if (!raw) { showToast('请输入数据', 'error'); return; }
  let ships;
  try { ships = JSON.parse(raw); } catch { showToast('JSON 格式错误', 'error'); return; }
  try {
    const resp = await apiFetch(`${API}/bulk`, {
      method: 'POST', body: JSON.stringify({ ships }),
    });
    showToast(resp.message);
    closeBulkModal();
    loadShips();
  } catch (e) {
    showToast(e.message, 'error');
  }
}

// ── 工具函数 ──
function escHtml(s) {
  const d = document.createElement('div');
  d.textContent = s;
  return d.innerHTML;
}
function escAttr(s) {
  return s.replace(/\\/g, '\\\\').replace(/"/g, '\\"').replace(/'/g, "\\'");
}

// ── 图片上传识别 ──
let uploadFile = null;

function openUploadModal() {
  uploadFile = null;
  document.getElementById('uploadFilename').textContent = '';
  document.getElementById('uploadPreview').style.display = 'none';
  document.getElementById('uploadPreview').src = '';
  document.getElementById('recognizeResult').classList.remove('show');
  document.getElementById('btnRecognize').style.display = '';
  document.getElementById('btnConfirmAdd').style.display = 'none';
  document.getElementById('btnRecognize').disabled = false;
  document.getElementById('btnConfirmAdd').disabled = false;
  document.getElementById('uploadModal').classList.add('active');
}

function closeUploadModal() {
  document.getElementById('uploadModal').classList.remove('active');
  uploadFile = null;
}

// 文件选择
document.getElementById('uploadFileInput').addEventListener('change', function (e) {
  if (e.target.files.length > 0) handleUploadFile(e.target.files[0]);
});

// 拖拽上传
const uploadZone = document.getElementById('uploadZone');
uploadZone.addEventListener('dragover', function (e) {
  e.preventDefault(); e.stopPropagation();
  this.classList.add('dragover');
});
uploadZone.addEventListener('dragleave', function (e) {
  e.preventDefault(); e.stopPropagation();
  this.classList.remove('dragover');
});
uploadZone.addEventListener('drop', function (e) {
  e.preventDefault(); e.stopPropagation();
  this.classList.remove('dragover');
  if (e.dataTransfer.files.length > 0) handleUploadFile(e.dataTransfer.files[0]);
});

function handleUploadFile(file) {
  if (!file.type.startsWith('image/')) {
    showToast('请选择图片文件', 'error');
    return;
  }
  if (file.size > 20 * 1024 * 1024) {
    showToast('文件过大，请上传 20MB 以内的图片', 'error');
    return;
  }
  uploadFile = file;
  document.getElementById('uploadFilename').textContent = file.name;

  const reader = new FileReader();
  reader.onload = function (e) {
    const img = document.getElementById('uploadPreview');
    img.src = e.target.result;
    img.style.display = 'block';
  };
  reader.readAsDataURL(file);

  document.getElementById('recognizeResult').classList.remove('show');
  document.getElementById('btnRecognize').style.display = '';
  document.getElementById('btnConfirmAdd').style.display = 'none';
}

async function doRecognize() {
  if (!uploadFile) { showToast('请先选择图片', 'error'); return; }

  const btn = document.getElementById('btnRecognize');
  const origText = btn.innerHTML;
  btn.disabled = true;
  btn.innerHTML = '<span class="loading-spinner"></span> 识别中…';

  try {
    const formData = new FormData();
    formData.append('file', uploadFile);

    const resp = await fetch(`${API}/recognize`, { method: 'POST', body: formData });
    const data = await resp.json();
    if (!resp.ok) throw new Error(data.detail || '识别失败');

    const d = data.data;
    document.getElementById('recHullNumber').value = d.hull_number || '';
    document.getElementById('recDescription').value = d.description || '';

    const warn = document.getElementById('recExistsWarn');
    if (d.already_exists) {
      warn.style.display = 'block';
      warn.textContent = `⚠️ 该弦号已存在于数据库中（原描述：${d.existing_description}），确认后将覆盖`;
    } else {
      warn.style.display = 'none';
    }

    document.getElementById('recognizeResult').classList.add('show');
    document.getElementById('btnConfirmAdd').style.display = '';
  } catch (e) {
    showToast('识别失败: ' + e.message, 'error');
  } finally {
    btn.disabled = false;
    btn.innerHTML = origText;
  }
}

async function doConfirmAdd() {
  const hn = document.getElementById('recHullNumber').value.trim();
  const desc = document.getElementById('recDescription').value.trim();
  if (!hn) { showToast('弦号不能为空', 'error'); return; }
  if (!desc) { showToast('描述不能为空', 'error'); return; }

  const btn = document.getElementById('btnConfirmAdd');
  const origText = btn.innerHTML;
  btn.disabled = true;
  btn.innerHTML = '<span class="loading-spinner"></span> 提交中…';

  try {
    const resp = await fetch(API, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ hull_number: hn, description: desc }),
    });
    const data = await resp.json();
    if (!resp.ok) throw new Error(data.detail || '添加失败');

    showToast(`✅ 已添加：弦号 ${hn}`);
    closeUploadModal();
    loadShips();
  } catch (e) {
    showToast(e.message, 'error');
  } finally {
    btn.disabled = false;
    btn.innerHTML = origText;
  }
}

// ── 初始化 ──
loadShips();
