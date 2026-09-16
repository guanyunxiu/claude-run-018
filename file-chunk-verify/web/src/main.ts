import './style.css';
import { uploadFileInChunks } from './uploader';
import type { UploadProgress, UploadResult } from './uploader';
import { ApiException } from './api';

const $ = <T extends HTMLElement>(id: string): T => {
  const el = document.getElementById(id);
  if (!el) throw new Error(`缺少 DOM 元素：#${id}`);
  return el as T;
};

const fileInput = $<HTMLInputElement>('fileInput');
const chunkSizeSelect = $<HTMLSelectElement>('chunkSizeSelect');
const startBtn = $<HTMLButtonElement>('startBtn');
const cancelBtn = $<HTMLButtonElement>('cancelBtn');
const fileMeta = $('fileMeta');
const hashBar = $('hashBar');
const upBar = $('upBar');
const hashText = $('hashText');
const upText = $('upText');
const pipelineText = $('pipelineText');
const hashStage = $('hashStage');
const upStage = $('upStage');
const logBox = $('log');
const resultBox = $('result');

const rFileId = $('rFileId');
const rChunks = $('rChunks');
const rUp = $('rUp');
const rAgg = $('rAgg');
const rMerged = $('rMerged');
const rPath = $('rPath');

let currentFile: File | null = null;
let abortController: AbortController | null = null;

function formatBytes(n: number): string {
  if (n === 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.min(units.length - 1, Math.floor(Math.log(n) / Math.log(1024)));
  return `${(n / 1024 ** i).toFixed(i === 0 ? 0 : 2)} ${units[i]}`;
}

function pct(part: number, total: number): number {
  if (total <= 0) return 100;
  return Math.min(100, (part / total) * 100);
}

function log(message: string): void {
  const time = new Date().toLocaleTimeString();
  logBox.textContent += `[${time}] ${message}\n`;
  logBox.scrollTop = logBox.scrollHeight;
}

function resetUI(): void {
  hashBar.style.width = '0%';
  upBar.style.width = '0%';
  hashText.textContent = '哈希进度：-';
  upText.textContent = '上传进度：-';
  pipelineText.textContent = '流水线：-';
  hashStage.textContent = '';
  upStage.textContent = '';
  resultBox.classList.add('hidden');
}

fileInput.addEventListener('change', () => {
  currentFile = fileInput.files?.[0] ?? null;
  resetUI();
  if (currentFile) {
    const f = currentFile;
    fileMeta.textContent =
      `${f.name} · ${formatBytes(f.size)} · 共 ` +
      `${f.size === 0 ? 0 : Math.ceil(f.size / Number(chunkSizeSelect.value))} 片（当前分片大小）`;
    startBtn.disabled = false;
    log(`已选择文件：${f.name}（${formatBytes(f.size)} 字节）`);
  } else {
    fileMeta.textContent = '尚未选择文件';
    startBtn.disabled = true;
  }
});

chunkSizeSelect.addEventListener('change', () => {
  if (currentFile) {
    const size = Number(chunkSizeSelect.value);
    fileMeta.textContent =
      `${currentFile.name} · ${formatBytes(currentFile.size)} · 共 ` +
      `${currentFile.size === 0 ? 0 : Math.ceil(currentFile.size / size)} 片（当前分片大小）`;
  }
});

function renderProgress(p: UploadProgress): void {
  hashBar.style.width = `${pct(p.hashedBytes, p.totalBytes)}%`;
  // 上传进度含“服务端已有跳过”部分（settledBytes 在启动时为 0，跳过通过事件累加）
  upBar.style.width = `${pct(p.settledBytes, p.totalBytes)}%`;

  hashText.textContent =
    `哈希进度：${p.hashedChunks}/${p.totalChunks} 片` +
    `（${formatBytes(p.hashedBytes)} / ${formatBytes(p.totalBytes)}）` +
    (p.hashCacheReused > 0 ? ` · IndexedDB 复用 ${p.hashCacheReused} 片` : '');

  const phaseName: Record<UploadProgress['phase'], string> = {
    init: '注册/恢复任务',
    'instant-done': '⚡ 秒传命中',
    pipeline: '边算边传',
    'locking-hash': '补报聚合哈希',
    completing: '服务端聚合校验中',
    done: '完成',
  };

  upText.textContent =
    `上传进度：${p.settledChunks}/${p.totalChunks} 片` +
    `（${formatBytes(p.settledBytes)} / ${formatBytes(p.totalBytes)}）` +
    ` · ${phaseName[p.phase]}` +
    (p.phase === 'pipeline' && p.newlyUploadedBytes > 0
      ? ` · 新传 ${formatBytes(p.newlyUploadedBytes)} @ ${formatBytes(
          Math.round(p.bytesPerSec),
        )}/s`
      : '');

  pipelineText.textContent =
    `流水线：哈希中 ${p.hashing ? '🟢' : '⚪'} ｜ 排队 ${p.queuedChunks} 片` +
    ` ｜ 上传中 ${p.uploading ? '🟢' : '⚪'} ｜ 在途槽位 ${p.inflightChunks}` +
    ` ｜ 本任务跳过 ${p.serverSkippedChunks} 片 ｜ 全局去重 ${p.globalDedupChunks} 片`;

  hashStage.textContent = p.hashing ? 'WORKING' : p.hashedChunks > 0 ? 'IDLE' : '';
  upStage.textContent = p.uploading ? 'UPLOADING' : p.settledChunks > 0 ? 'IDLE' : '';
}

function showResult(result: UploadResult): void {
  const f = result.init.file;
  rFileId.textContent = f.fileId;
  rChunks.textContent =
    `${f.totalChunks} 片（IndexedDB 复用哈希 ${result.hashCacheReused} 片` +
    (result.instant ? ' · ⚡ 秒传' : '') +
    '）';
  rUp.textContent = result.instant
    ? `⚡ 秒传：新传 0 片 / 复用服务端 ${f.totalChunks} 片（全局去重 ${result.globalDedupChunks} 片）`
    : `新传 ${result.newlyUploadedChunks} 片 / 本任务跳过 ${result.serverSkippedChunks} 片 / 全局 CAS 去重 ${result.globalDedupChunks} 片`;
  // 秒传时没有本次 complete 响应，用 init 中已锁定的文件/合并哈希展示
  rAgg.textContent = result.complete?.aggregateHash ?? f.fileHash ?? '(复用已有)';
  rMerged.textContent = result.complete?.mergedHash ?? f.mergedHash ?? '(复用已有)';
  rPath.textContent = result.complete?.mergedPath ?? f.mergedPath ?? '(复用已有)';
  resultBox.classList.remove('hidden');
}

function setRunning(running: boolean): void {
  startBtn.disabled = running || !currentFile;
  cancelBtn.disabled = !running;
  fileInput.disabled = running;
  chunkSizeSelect.disabled = running;
}

startBtn.addEventListener('click', async () => {
  if (!currentFile) return;
  resetUI();
  abortController = new AbortController();
  setRunning(true);

  const chunkSize = Number(chunkSizeSelect.value);
  try {
    const result = await uploadFileInChunks({
      file: currentFile,
      chunkSize,
      maxInflight: 6,
      uploadConcurrency: 3,
      signal: abortController.signal,
      onLog: log,
      onProgress: renderProgress,
    });
    showResult(result);
  } catch (err) {
    if (err instanceof DOMException && err.name === 'AbortError') {
      log('已取消。本地哈希缓存与服务端已传分片均保留，再次开始将自动续算、续传。');
    } else if (err instanceof ApiException) {
      log(`接口错误 [${err.code}] HTTP ${err.status}：${err.message}`);
      if (err.details) log(`详情：${JSON.stringify(err.details)}`);
    } else if (err instanceof Error) {
      log(`失败：${err.message}`);
    } else {
      log(`失败：${String(err)}`);
    }
  } finally {
    setRunning(false);
    abortController = null;
  }
});

cancelBtn.addEventListener('click', () => {
  abortController?.abort();
});
