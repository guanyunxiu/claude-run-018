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
const logBox = $('log');
const resultBox = $('result');

const rFileId = $('rFileId');
const rChunks = $('rChunks');
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
  upBar.style.width = `${pct(p.settledBytes, p.totalBytes)}%`;

  hashText.textContent =
    `哈希进度：${p.hashedChunks}/${p.totalChunks} 片` +
    `（${formatBytes(p.hashedBytes)} / ${formatBytes(p.totalBytes)}）`;

  const phaseName: Record<UploadProgress['phase'], string> = {
    hashing: '哈希计算中',
    init: '注册/恢复任务',
    uploading: '上传中',
    completing: '服务端聚合校验中',
    done: '完成',
  };
  upText.textContent =
    `上传进度：${p.settledChunks}/${p.totalChunks} 片` +
    `（${formatBytes(p.settledBytes)} / ${formatBytes(p.totalBytes)}）` +
    ` · 状态：${phaseName[p.phase]}` +
    (p.phase === 'uploading' && p.newlyUploadedBytes > 0
      ? ` · 本次新传 ${formatBytes(p.newlyUploadedBytes)} @ ${formatBytes(
          Math.round(p.bytesPerSec),
        )}/s`
      : '');
}

function showResult(result: UploadResult): void {
  rFileId.textContent = result.complete.fileId;
  rChunks.textContent = `${result.complete.totalChunks} 片（断点跳过 ${result.skippedChunks} 片）`;
  rAgg.textContent = result.complete.aggregateHash;
  rMerged.textContent = result.complete.mergedHash;
  rPath.textContent = result.complete.mergedPath;
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
      concurrency: 3,
      signal: abortController.signal,
      onLog: log,
      onProgress: renderProgress,
    });
    showResult(result);
  } catch (err) {
    if (err instanceof DOMException && err.name === 'AbortError') {
      log('已取消。已上传的分片保留在服务端，再次开始将自动跳过。');
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
