// Хранилище виртуальных дисков/ISO на IndexedDB (sparse-блоки).

(() => {
  "use strict";

  const DB_NAME = "osemu-machines";
  const DB_VERSION = 2;
  const KB = 1024, MB = 1024 * KB, GB = 1024 * MB;

  const BLOCK_SIZE = 64 * KB;          // размер блока по умолчанию (у существующих дисков берётся из записи)
  const CACHE_LIMIT = 16 * MB;         // лимит кэша на устройство
  const MAX_IO_SIZE = 64 * MB;         // защита от абсурдных запросов
  const MAX_IMAGE_BYTES = 64 * GB;
  const LOCK_TIMEOUT = 120_000;        

  const FLUSH_DELAY = 250;             // задержка сброса в IndexedDB
  const FLUSH_EAGER_BYTES = 8 * MB;    // лимит буфера 
  const DIRTY_HIGH_WATER = 48 * MB;    // 
  const COMMIT_BATCH_BYTES = 8 * MB;   // размер одной транзакции записи
  const IMPORT_BATCH_BYTES = 8 * MB;
  const RETRY_DELAY = 3000, MAX_RETRIES = 5;
  const nativeLockHolds = new Map();

  async function acquireNativeLocks(ids, owner) {
    if (!navigator.locks?.request) return false;
    const holds = [];
    try {
      for (const id of ids) {
        let resolveAcquired;
        const acquired = new Promise(resolve => { resolveAcquired = resolve; });
        let releaseHold;
        const request = navigator.locks.request(`osemu-disk:${id}`, { mode: "exclusive", ifAvailable: true }, lock => {
          if (!lock) {
            resolveAcquired(false);
            return;
          }
          return new Promise(resolve => {
            releaseHold = resolve;
            resolveAcquired(true);
          });
        });
        request.catch(error => {
          resolveAcquired(false);
          console.error("Could not acquire the browser disk lock.", error);
        });
        if (!await acquired) throw new Error("Диск сейчас используется другой запущенной машиной.");
        holds.push({ id, release: releaseHold, request });
      }
      nativeLockHolds.set(owner, holds);
      return true;
    } catch (error) {
      for (const hold of holds) hold.release();
      await Promise.allSettled(holds.map(hold => hold.request));
      throw error;
    }
  }

  async function releaseNativeLocks(owner) {
    const holds = nativeLockHolds.get(owner);
    if (!holds) return;
    nativeLockHolds.delete(owner);
    for (const hold of holds) hold.release();
    await Promise.allSettled(holds.map(hold => hold.request));
  }

  /* ---------- утилиты ---------- */

  const makeId = prefix => `${prefix}-${crypto.randomUUID?.() || `${Date.now()}-${Math.random().toString(36).slice(2)}`}`;
  const roundUp512 = value => Math.ceil(value / 512) * 512;
  const blockRange = id => IDBKeyRange.bound([id, 0], [id, Number.MAX_SAFE_INTEGER]);

  const requestResult = request => new Promise((resolve, reject) => {
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error || new Error("IndexedDB request failed"));
  });

  const transactionDone = transaction => {
    const promise = new Promise((resolve, reject) => {
      transaction.oncomplete = () => resolve();
      transaction.onabort = () => reject(transaction.error || new DOMException("Transaction aborted", "AbortError"));
    });
    promise.catch(() => {}); // чтобы ранний reject не давал unhandledrejection
    return promise;
  };

  const toBytes = input => input instanceof Uint8Array
    ? input
    : ArrayBuffer.isView(input)
      ? new Uint8Array(input.buffer, input.byteOffset, input.byteLength)
      : new Uint8Array(input);

  function isZero(bytes) {
    const length = bytes.byteLength;
    let index = 0;
    if ((bytes.byteOffset & 3) === 0) {
      const words = new Uint32Array(bytes.buffer, bytes.byteOffset, length >> 2);
      for (; index < words.length; index++) if (words[index] !== 0) return false;
      index <<= 2;
    }
    for (; index < length; index++) if (bytes[index] !== 0) return false;
    return true;
  }

  function normalizeError(error) {
    if (error?.name === "QuotaExceededError") return new Error("Недостаточно места в хранилище браузера. Освободите место и повторите операцию.");
    return error instanceof Error ? error : new Error(error?.message || String(error || "Ошибка IndexedDB"));
  }

  function groupRuns(indices) {
    const sorted = [...new Set(indices)].sort((a, b) => a - b), runs = [];
    for (const index of sorted) {
      const run = runs[runs.length - 1];
      if (run && index === run[1] + 1) run[1] = index; else runs.push([index, index]);
    }
    return runs;
  }

  function patchRange(offset, source, blockSize, diskSize, getBlock, onBlock) {
    const end = offset + source.byteLength;
    const last = Math.floor((end - 1) / blockSize);
    for (let index = Math.floor(offset / blockSize); index <= last; index++) {
      const start = index * blockSize, length = Math.min(blockSize, diskSize - start);
      const from = Math.max(offset, start), to = Math.min(end, start + length);
      const data = getBlock(index, length, from === start && to === start + length);
      data.set(source.subarray(from - offset, to - offset), from - start);
      onBlock(index, data);
    }
  }

  function blobReader(blob, chunk = 4 * MB) {
    let offset = 0;
    return {
      async read() {
        if (offset >= blob.size) return { done: true };
        const part = blob.slice(offset, offset + chunk);
        offset += chunk;
        return { done: false, value: new Uint8Array(await part.arrayBuffer()) };
      },
      cancel: () => Promise.resolve(),
    };
  }

  // режет поток на блоки фиксированного размера и отдаёт их в синк
  async function consumeStream(reader, blockSize, align, limit, sink, onProgress) {
    let current = new Uint8Array(blockSize), fill = 0, index = 0, total = 0;
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      total += value.byteLength;
      if (total > limit) throw new Error("Образ превышает лимит 64 ГБ.");
      for (let position = 0; position < value.byteLength;) {
        const count = Math.min(blockSize - fill, value.byteLength - position);
        current.set(value.subarray(position, position + count), fill);
        fill += count; position += count;
        if (fill === blockSize) {
          const kept = await sink.push(index++, current);
          if (kept) current = new Uint8Array(blockSize);
          fill = 0;
        }
      }
      onProgress?.(total);
    }
    if (fill) {
      const last = new Uint8Array(Math.ceil(fill / align) * align);
      last.set(current.subarray(0, fill));
      await sink.push(index, last);
    }
    await sink.finish();
    return total;
  }

  function makeSink(commit) {
    let batch = [], bytes = 0;
    const flush = async () => {
      if (!batch.length) return;
      const entries = batch;
      batch = []; bytes = 0;
      await commit(entries);
    };
    return {
      async push(index, data) {
        if (isZero(data)) return false; // sparse
        batch.push([index, data]); bytes += data.byteLength;
        if (bytes >= IMPORT_BATCH_BYTES) await flush();
        return true;
      },
      finish: flush,
    };
  }

  /* ---------- база ---------- */

  let databasePromise;
  function openDatabase() {
    if (databasePromise) return databasePromise;
    databasePromise = new Promise((resolve, reject) => {
      const request = indexedDB.open(DB_NAME, DB_VERSION);
      request.onupgradeneeded = () => {
        const db = request.result;
        if (!db.objectStoreNames.contains("machines")) db.createObjectStore("machines", { keyPath: "id" });
        if (!db.objectStoreNames.contains("disks")) db.createObjectStore("disks", { keyPath: "id" });
        if (!db.objectStoreNames.contains("diskBlocks")) db.createObjectStore("diskBlocks", { keyPath: ["diskId", "index"] });
        if (!db.objectStoreNames.contains("media")) db.createObjectStore("media", { keyPath: "id" });
        if (!db.objectStoreNames.contains("mediaBlocks")) db.createObjectStore("mediaBlocks", { keyPath: ["mediaId", "index"] });
        if (!db.objectStoreNames.contains("locks")) db.createObjectStore("locks", { keyPath: "diskId" });
      };
      request.onsuccess = () => {
        const db = request.result;
        const reset = () => { if (databasePromise) databasePromise = null; };
        db.onversionchange = () => { db.close(); reset(); };
        db.onclose = reset;
        navigator.storage?.persist?.().catch(() => {}); 
        resolve(db);
      };
      request.onerror = () => reject(request.error || new Error("Could not open the virtual disk database"));
      request.onblocked = () => reject(new Error("Close other emulator tabs to initialize virtual disk storage"));
    }).catch(error => {
      databasePromise = null;
      throw error;
    });
    return databasePromise;
  }

  /* ---------- хранилище ---------- */

  class DiskStore {
    // run(tx) может быть async; транзакция откатывается при любой ошибке.
    async transaction(stores, mode, run, options) {
      const db = await openDatabase();
      const tx = db.transaction(stores, mode, options);
      const done = transactionDone(tx);
      try {
        const value = await run(tx);
        await done;
        return value;
      } catch (error) {
        try { tx.abort(); } catch {}
        await done.catch(() => {});
        throw normalizeError(error);
      }
    }

    async listDisks() {
      const [disks, locks] = await this.transaction(["disks", "locks"], "readonly", tx => Promise.all([
        requestResult(tx.objectStore("disks").getAll()),
        requestResult(tx.objectStore("locks").getAll()),
      ]));
      const now = Date.now();
      const lockMap = new Map(locks.filter(lock => lock.expiresAt > now).map(lock => [lock.diskId, lock]));
      return disks.map(disk => ({ ...disk, lock: lockMap.get(disk.id) || null })).sort((a, b) => a.createdAt - b.createdAt);
    }

    async getDisk(id) {
      const disk = await this.transaction("disks", "readonly", tx => requestResult(tx.objectStore("disks").get(id)));
      if (!disk) throw new Error("Виртуальный диск не найден.");
      return disk;
    }

    async createDisk(name, sizeBytes) {
      const normalizedName = String(name || "").trim();
      if (!normalizedName) throw new Error("Укажите название диска.");
      if (!Number.isSafeInteger(sizeBytes) || sizeBytes < 512 || sizeBytes % 512 !== 0 || sizeBytes > MAX_IMAGE_BYTES) {
        throw new Error("Размер диска должен быть кратен 512 байтам и не превышать 64 ГБ.");
      }
      const record = {
        id: makeId("disk"),
        name: normalizedName,
        sizeBytes,
        blockSize: BLOCK_SIZE,
        allocatedBlocks: 0,
        createdAt: Date.now(),
        format: "sparse-v1",
      };
      await this.transaction("disks", "readwrite", tx => requestResult(tx.objectStore("disks").add(record)));
      return record;
    }

    async renameDisk(id, name) {
      const normalizedName = String(name || "").trim();
      if (!normalizedName) throw new Error("Укажите название диска.");
      return this.transaction("disks", "readwrite", async tx => {
        const store = tx.objectStore("disks");
        const disk = await requestResult(store.get(id));
        if (!disk) throw new Error("Виртуальный диск не найден.");
        disk.name = normalizedName;
        store.put(disk);
        return disk;
      });
    }

    async setDiskSize(id, sizeBytes) {
      await this.transaction("disks", "readwrite", async tx => {
        const store = tx.objectStore("disks");
        const disk = await requestResult(store.get(id));
        if (!disk) throw new Error("Виртуальный диск не найден.");
        disk.sizeBytes = Math.max(512, sizeBytes);
        store.put(disk);
      });
    }

    async importUrlDisk(url, name, onProgress) {
      const response = await fetch(url);
      if (!response.ok) throw new Error(`Не удалось загрузить образ (${response.status}).`);
      if (!response.body) throw new Error("Браузер не поддерживает потоковый импорт этого образа.");
      const declaredSize = Number(response.headers.get("content-length")) || 0;
      if (declaredSize > MAX_IMAGE_BYTES) throw new Error("Образ превышает лимит виртуального диска 64 ГБ.");
      const disk = await this.createDisk(name, declaredSize ? roundUp512(declaredSize) : MAX_IMAGE_BYTES);
      const reader = response.body.getReader();
      try {
        const total = await consumeStream(
          reader, BLOCK_SIZE, 512, MAX_IMAGE_BYTES,
          makeSink(entries => this.commitBlocks(disk.id, entries)),
          done => onProgress?.(done, declaredSize),
        );
        if (!total) throw new Error("Образ пуст.");
        await this.setDiskSize(disk.id, roundUp512(total));
        return await this.getDisk(disk.id);
      } catch (error) {
        reader.cancel().catch(() => {});
        await this.deleteDisk(disk.id).catch(cleanupError => console.error("Could not clean up an incomplete disk import.", cleanupError));
        throw normalizeError(error);
      }
    }

    async importDiskFile(file, name, onProgress) {
      if (!(file instanceof Blob) || !file.size) throw new Error("Файл пуст или недоступен.");
      const disk = await this.createDisk(name || file.name.replace(/\.[^.]+$/, ""), roundUp512(file.size));
      try {
        await consumeStream(
          blobReader(file), BLOCK_SIZE, 512, MAX_IMAGE_BYTES,
          makeSink(entries => this.commitBlocks(disk.id, entries)),
          done => onProgress?.(Math.min(done, file.size), file.size),
        );
        return await this.getDisk(disk.id);
      } catch (error) {
        await this.deleteDisk(disk.id).catch(cleanupError => console.error("Could not clean up an incomplete disk import.", cleanupError));
        throw normalizeError(error);
      }
    }

    async deleteDisk(id) {
      const owner = `delete:${makeId("disk")}`;
      const hasNativeLock = await acquireNativeLocks([id], owner);
      try {
        await this.transaction(["disks", "diskBlocks", "locks", "machines"], "readwrite", async tx => {
          const locks = tx.objectStore("locks");
          const lock = await requestResult(locks.get(id));
          if (!hasNativeLock && lock && lock.expiresAt > Date.now()) {
            throw new Error("Этот диск подключён к работающей виртуальной машине.");
          }
          const disks = tx.objectStore("disks");
          if (!await requestResult(disks.get(id))) throw new Error("Виртуальный диск уже удалён.");

          tx.objectStore("diskBlocks").delete(blockRange(id));
          disks.delete(id);
          locks.delete(id);

          const machines = tx.objectStore("machines");
          for (const machine of await requestResult(machines.getAll())) {
            let changed = false;
            for (const field of ["diskId", "secondaryDiskId", "floppyDiskId"]) {
              if (machine[field] === id) {
                machine[field] = null;
                changed = true;
              }
            }
            if (machine.bootSource?.type === "catalog-disk" && !machine.diskId) {
              machine.bootSource = null;
              changed = true;
            }
            if (changed) {
              machine.updatedAt = Date.now();
              machines.put(machine);
            }
          }
        });
      } finally {
        if (hasNativeLock) await releaseNativeLocks(owner);
      }
    }

    /* --- машины --- */

    async listMachines() {
      const machines = await this.transaction("machines", "readonly", tx => requestResult(tx.objectStore("machines").getAll()));
      return machines.sort((a, b) => a.createdAt - b.createdAt);
    }

    async saveMachine(machine) {
      const record = { ...machine, updatedAt: Date.now() };
      await this.transaction("machines", "readwrite", tx => { tx.objectStore("machines").put(record); });
      return record;
    }

    async deleteMachine(id) {
      await this.transaction("machines", "readwrite", tx => { tx.objectStore("machines").delete(id); });
    }

    /* --- установочные образы --- */

    async addMedia(file, kind, onProgress) {
      if (!(file instanceof Blob) || !file.size) throw new Error("Файл пуст или недоступен.");
      if (file.size > MAX_IMAGE_BYTES) throw new Error("Размер установочного образа не может превышать 64 ГБ.");
      const record = { id: makeId("media"), name: file.name || "Образ", type: file.type || "", kind, sizeBytes: file.size, blockSize: BLOCK_SIZE, createdAt: Date.now(), importing: true };
      await this.transaction("media", "readwrite", tx => requestResult(tx.objectStore("media").add(record)));
      try {
        await consumeStream(
          blobReader(file), BLOCK_SIZE, 1, MAX_IMAGE_BYTES,
          makeSink(entries => this.putMediaBlocks(record.id, entries)),
          done => onProgress?.(Math.min(done, file.size), file.size),
        );
        record.importing = false;
        await this.transaction("media", "readwrite", tx => { tx.objectStore("media").put(record); });
        return record;
      } catch (error) {
        await this.discardMedia(record.id);
        throw normalizeError(error);
      }
    }

    async importUrlMedia(url, name, onProgress) {
      const response = await fetch(url);
      if (!response.ok) throw new Error(`Не удалось загрузить ISO (${response.status}).`);
      if (!response.body) throw new Error("Браузер не поддерживает потоковую загрузку ISO.");
      const declaredSize = Number(response.headers.get("content-length")) || 0;
      if (declaredSize > MAX_IMAGE_BYTES) throw new Error("Размер ISO превышает 64 ГБ.");
      const record = { id: makeId("media"), name, type: "application/octet-stream", kind: "iso", sizeBytes: 0, blockSize: BLOCK_SIZE, createdAt: Date.now(), importing: true };
      await this.transaction("media", "readwrite", tx => requestResult(tx.objectStore("media").add(record)));
      const reader = response.body.getReader();
      try {
        record.sizeBytes = await consumeStream(
          reader, BLOCK_SIZE, 1, MAX_IMAGE_BYTES,
          makeSink(entries => this.putMediaBlocks(record.id, entries)),
          done => onProgress?.(done, declaredSize),
        );
        if (!record.sizeBytes) throw new Error("ISO пуст.");
        record.importing = false;
        await this.transaction("media", "readwrite", tx => { tx.objectStore("media").put(record); });
        return record;
      } catch (error) {
        reader.cancel().catch(() => {});
        await this.discardMedia(record.id);
        throw normalizeError(error);
      }
    }

    async discardMedia(id) {
      await this.deleteMediaChunks(id).catch(error => console.error("Could not clean up an incomplete ISO import.", error));
      await this.transaction("media", "readwrite", tx => { tx.objectStore("media").delete(id); }).catch(() => {});
    }

    async putMediaBlocks(id, entries) {
      await this.transaction("mediaBlocks", "readwrite", tx => {
        const store = tx.objectStore("mediaBlocks");
        for (const [index, data] of entries) store.put({ mediaId: id, index, data });
      }, { durability: "relaxed" });
    }

    async listMedia() {
      const media = await this.transaction("media", "readonly", tx => requestResult(tx.objectStore("media").getAll()));
      return media.filter(item => !item.importing).sort((a, b) => a.createdAt - b.createdAt);
    }

    async getMedia(id) {
      const media = await this.transaction("media", "readonly", tx => requestResult(tx.objectStore("media").get(id)));
      if (!media || media.importing) throw new Error("Загрузочный образ не найден или ещё импортируется.");
      return media;
    }

    async deleteMedia(id) {
      await this.transaction(["media", "mediaBlocks", "machines"], "readwrite", async tx => {
        const machines = await requestResult(tx.objectStore("machines").getAll());
        if (machines.some(machine => machine.mediaId === id)) throw new Error("Сначала отключите ISO от сохранённых виртуальных машин.");
        tx.objectStore("media").delete(id);
        tx.objectStore("mediaBlocks").delete(blockRange(id));
      });
    }

    async deleteMediaChunks(id) {
      await this.transaction("mediaBlocks", "readwrite", tx => { tx.objectStore("mediaBlocks").delete(blockRange(id)); });
    }

    /* --- блокировки --- */

    async acquireLocks(diskIds, owner) {
      const ids = [...new Set(diskIds.filter(Boolean))].sort();
      if (!ids.length) return [];
      const hasNativeLock = await acquireNativeLocks(ids, owner);
      try {
        return await this.transaction(["locks", "disks"], "readwrite", async tx => {
          const locks = tx.objectStore("locks"), disks = tx.objectStore("disks");
          const [lockRows, diskRows] = await Promise.all([
            Promise.all(ids.map(id => requestResult(locks.get(id)))),
            Promise.all(ids.map(id => requestResult(disks.get(id)))),
          ]);
          const now = Date.now();
          if (diskRows.some(row => !row)) throw new Error("Один из выбранных виртуальных дисков не найден.");
          if (!hasNativeLock && lockRows.some(row => row && row.expiresAt > now && row.owner !== owner)) {
            throw new Error("Диск сейчас используется другой запущенной машиной.");
          }
          for (const diskId of ids) locks.put({ diskId, owner, expiresAt: now + LOCK_TIMEOUT });
          return ids;
        });
      } catch (error) {
        if (hasNativeLock) await releaseNativeLocks(owner);
        throw error;
      }
    }

    async refreshLocks(ids, owner) {
      if (!ids.length) return true;
      return this.transaction("locks", "readwrite", async tx => {
        const locks = tx.objectStore("locks");
        const rows = await Promise.all(ids.map(id => requestResult(locks.get(id))));
        const expiresAt = Date.now() + LOCK_TIMEOUT;
        let valid = true;
        for (const row of rows) {
          if (!row || row.owner !== owner) { valid = false; continue; }
          row.expiresAt = expiresAt;
          locks.put(row);
        }
        return valid;
      });
    }

    async releaseLocks(ids, owner) {
      try {
        if (ids.length) {
          await this.transaction("locks", "readwrite", async tx => {
            const locks = tx.objectStore("locks");
            const rows = await Promise.all(ids.map(id => requestResult(locks.get(id))));
            rows.forEach((row, i) => { if (row?.owner === owner) locks.delete(ids[i]); });
          });
        }
      } finally {
        await releaseNativeLocks(owner);
      }
    }

    /* --- блоки --- */

    // Читает блоки одним getAll на каждый непрерывный отрезок. Отсутствующие блоки в результат не попадают (нули).
    async readBlockRuns(storeName, id, indices) {
      const result = new Map();
      if (!indices.length) return result;
      const lists = await this.transaction(storeName, "readonly", tx => {
        const store = tx.objectStore(storeName);
        return Promise.all(groupRuns(indices).map(([from, to]) => requestResult(store.getAll(IDBKeyRange.bound([id, from], [id, to])))));
      });
      for (const list of lists) {
        for (const record of list) result.set(record.index, record.data instanceof Uint8Array ? record.data : new Uint8Array(record.data));
      }
      return result;
    }

    readBlocks(id, indices) { return this.readBlockRuns("diskBlocks", id, indices); }
    readMediaBlocks(id, indices) { return this.readBlockRuns("mediaBlocks", id, indices); }

    // Одна транзакция на пачку блоков: нулевые блоки удаляются, остальные пишутся, счётчик allocatedBlocks обновляется.
    async commitBlocks(id, entries) {
      if (!entries.length) return;
      await this.transaction(["diskBlocks", "disks"], "readwrite", async tx => {
        const blocks = tx.objectStore("diskBlocks"), disks = tx.objectStore("disks");
        const disk = await requestResult(disks.get(id));
        if (!disk) throw new Error("Виртуальный диск не найден.");
        const existing = await Promise.all(entries.map(([index]) => requestResult(blocks.getKey([id, index]))));
        let delta = 0;
        entries.forEach(([index, data], i) => {
          const had = existing[i] !== undefined;
          if (isZero(data)) {
            if (had) { blocks.delete([id, index]); delta--; }
          } else {
            blocks.put({ diskId: id, index, data });
            if (!had) delta++;
          }
        });
        disk.allocatedBlocks = Math.max(0, (disk.allocatedBlocks || 0) + delta);
        disks.put(disk);
      }, { durability: "relaxed" });
    }

    // Совместимость со старым API: запись произвольного диапазона напрямую в хранилище.
    async write(id, offset, input) {
      const source = toBytes(input).slice();
      if (!Number.isSafeInteger(offset) || offset < 0 || !source.byteLength || source.byteLength > MAX_IO_SIZE) throw new Error("Недопустимый размер операции записи на виртуальный диск.");
      const disk = await this.getDisk(id);
      if (offset + source.byteLength > disk.sizeBytes) throw new Error("Запись выходит за границы виртуального диска.");
      const blockSize = disk.blockSize || BLOCK_SIZE;
      const first = Math.floor(offset / blockSize), last = Math.floor((offset + source.byteLength - 1) / blockSize);
      const needed = [];
      for (let index = first; index <= last; index++) needed.push(index);
      const existing = await this.readBlocks(id, needed);
      const entries = [], updates = new Map();
      patchRange(offset, source, blockSize, disk.sizeBytes,
        (index, length) => {
          const old = existing.get(index);
          const data = new Uint8Array(length);
          if (old) data.set(old.subarray(0, length));
          return data;
        },
        (index, data) => { entries.push([index, data]); updates.set(index, isZero(data) ? null : data); });
      await this.commitBlocks(id, entries);
      return { updates };
    }
  }

  /* ---------- устройство для эмулятора ---------- */

  const liveDevices = new Set();
  const flushAll = () => {
    for (const ref of liveDevices) {
      const device = ref.deref();
      if (!device) liveDevices.delete(ref);
      else device.flush().catch(() => {});
    }
  };
  document.addEventListener("visibilitychange", () => { if (document.visibilityState === "hidden") flushAll(); });
  window.addEventListener("pagehide", flushAll);

  class IndexedDBDiskDevice {
    constructor(store, id, cacheLimit = CACHE_LIMIT) {
      this.store = store; this.id = id; this.cacheLimit = cacheLimit;
      this.blockSize = BLOCK_SIZE;
      this.cache = new Map(); this.cacheBytes = 0;          // все резидентные блоки (чистые и грязные), порядок = LRU
      this.dirty = new Set(); this.committing = new Set();  // эти блоки из кэша не вытесняются
      this.queue = Promise.resolve(); this.pending = 0;
      this.flushTimer = 0; this.flushChain = Promise.resolve(); this.failures = 0;
      this.closed = false; this.ref = null;
      this.byteLength = 0; this.onload = null; this.onerror = null;
      this.diagnostics = { readBytes: 0, writtenBytes: 0, committedBytes: 0, peakTemporaryBytes: 0, temporaryBytes: 0, cacheLimitBytes: cacheLimit };
    }

    async load() {
      const disk = await this.store.getDisk(this.id);
      this.byteLength = disk.sizeBytes;
      this.blockSize = disk.blockSize || BLOCK_SIZE;
      this.register();
      this.onload?.({ buffer: null });
    }

    register() {
      if (this.ref) return;
      this.ref = new WeakRef(this);
      liveDevices.add(this.ref);
    }

    /* --- вспомогательное --- */

    blockLength(index) { return Math.min(this.blockSize, this.byteLength - index * this.blockSize); }

    validRange(offset, length) {
      return Number.isSafeInteger(offset) && Number.isSafeInteger(length) && offset >= 0 && length >= 0 && length <= MAX_IO_SIZE && offset + length <= this.byteLength;
    }

    track(length) {
      const d = this.diagnostics;
      d.temporaryBytes += length;
      d.peakTemporaryBytes = Math.max(d.peakTemporaryBytes, d.temporaryBytes);
      return () => { d.temporaryBytes -= length; };
    }

    // очередтьп записи
    run(task) {
      this.pending++;
      const result = this.queue.then(task);
      this.queue = result.then(() => {}, () => {});
      return result.finally(() => { this.pending--; });
    }

    touch(index) {
      const data = this.cache.get(index);
      if (data) { this.cache.delete(index); this.cache.set(index, data); }
      return data;
    }

    putCache(index, data) {
      const previous = this.cache.get(index);
      if (previous) { this.cacheBytes -= previous.byteLength; this.cache.delete(index); }
      this.cache.set(index, data);
      this.cacheBytes += data.byteLength;
    }

    evict() {
      if (this.cacheBytes <= this.cacheLimit) return;
      for (const [index, data] of this.cache) {
        if (this.cacheBytes <= this.cacheLimit) break;
        if (this.dirty.has(index) || this.committing.has(index)) continue;
        this.cache.delete(index);
        this.cacheBytes -= data.byteLength;
      }
    }

    normalizeBlock(index, data) {
      const length = this.blockLength(index);
      if (data.byteLength === length && data.byteOffset === 0 && data.buffer.byteLength === length) return data;
      const fixed = new Uint8Array(length); // копия: нужная длина и без чужого большого ArrayBuffer
      fixed.set(data.subarray(0, Math.min(length, data.byteLength)));
      return fixed;
    }

    fetchBlocks(indices) { return this.store.readBlocks(this.id, indices); }

    async loadBlocks(indices) {
      const result = new Map(), missing = [];
      for (const index of indices) {
        const hit = this.touch(index);
        if (hit) result.set(index, hit); else missing.push(index);
      }
      if (missing.length) {
        const fetched = await this.fetchBlocks(missing);
        for (const index of missing) {
          const data = this.normalizeBlock(index, fetched.get(index) || new Uint8Array(this.blockLength(index)));
          result.set(index, data);
          this.putCache(index, data);
        }
        this.evict();
      }
      return result;
    }

    gather(offset, length, lookup) {
      const output = new Uint8Array(length), end = offset + length;
      for (let position = offset; position < end;) {
        const index = Math.floor(position / this.blockSize), data = lookup(index);
        if (!data) return undefined;
        const inner = position - index * this.blockSize, count = Math.min(data.byteLength - inner, end - position);
        if (count <= 0) return undefined;
        output.set(data.subarray(inner, inner + count), position - offset);
        position += count;
      }
      return output;
    }

    /* --- чтение --- */

    get_from_cache(offset, length) {
      if (this.pending || !this.validRange(offset, length)) return undefined;
      if (!length) return new Uint8Array();
      return this.gather(offset, length, index => this.touch(index));
    }

    get(offset, length, callback) {
      if (!this.validRange(offset, length)) {
        this.fail(new Error("Запрос чтения выходит за границы виртуального диска."));
        return;
      }
      if (!length) { callback(new Uint8Array(0)); return; }
      const cached = this.get_from_cache(offset, length);
      if (cached) {
        this.diagnostics.readBytes += length;
        callback(cached);
        return;
      }
      const release = this.track(length);
      this.run(() => this.readRange(offset, length)).then(data => {
        release();
        this.diagnostics.readBytes += length;
        callback(data);
      }, error => {
        release();
        this.fail(error);
      });
    }

    async readRange(offset, length) {
      const first = Math.floor(offset / this.blockSize), last = Math.floor((offset + length - 1) / this.blockSize);
      const indices = [];
      for (let index = first; index <= last; index++) indices.push(index);
      const loaded = await this.loadBlocks(indices);
      return this.gather(offset, length, index => loaded.get(index));
    }

    get_and_cache(offset, length, callback) { this.get(offset, length, callback); }

    /* --- запись --- */

    blocksToLoad(offset, length) {
      const end = offset + length, need = [];
      for (let index = Math.floor(offset / this.blockSize), last = Math.floor((end - 1) / this.blockSize); index <= last; index++) {
        if (this.cache.has(index)) continue;
        const start = index * this.blockSize;
        if (offset <= start && end >= start + this.blockLength(index)) continue;
        need.push(index);
      }
      return need;
    }

    applyWrite(offset, source, loaded) {
      patchRange(offset, source, this.blockSize, this.byteLength,
        (index, length, whole) => {
          let data = this.cache.get(index) || loaded?.get(index);
          if (!data) {
            if (!whole) throw new Error("Блок не был загружен перед записью.");
            data = new Uint8Array(length);
          }
          return data;
        },
        (index, data) => {
          if (this.cache.get(index) !== data) this.putCache(index, data); else this.touch(index);
          this.dirty.add(index);
        });
      this.evict();
      this.scheduleFlush();
    }

    set(offset, input, callback) {
      const source = toBytes(input);
      if (!Number.isSafeInteger(offset) || offset < 0 || offset + source.byteLength > this.byteLength || source.byteLength > MAX_IO_SIZE) {
        this.fail(new Error("Запись выходит за границы виртуального диска."));
        return;
      }
      if (!source.byteLength) { callback(); return; }

      const overloaded = this.dirty.size * this.blockSize >= DIRTY_HIGH_WATER;
      if (!this.pending && !overloaded && !this.blocksToLoad(offset, source.byteLength).length) {
        this.applyWrite(offset, source);
        this.diagnostics.writtenBytes += source.byteLength;
        callback();
        return;
      }

      // Эмулятор может переиспользовать свой буфер сразу после вызова — копируем до первого await.
      const copy = source.slice();
      const release = this.track(copy.byteLength);
      this.run(async () => {
        const need = this.blocksToLoad(offset, copy.byteLength);
        const loaded = need.length ? await this.loadBlocks(need) : undefined;
        this.applyWrite(offset, copy, loaded);
        if (this.dirty.size * this.blockSize >= DIRTY_HIGH_WATER) await this.flush(); // backpressure
      }).then(() => {
        release();
        this.diagnostics.writtenBytes += copy.byteLength;
        callback();
      }, error => {
        release();
        this.fail(error);
      });
    }

    /* --- сброс в IndexedDB --- */

    scheduleFlush(delay) {
      if (this.flushTimer || this.closed) return;
      if (delay === undefined) delay = this.dirty.size * this.blockSize >= FLUSH_EAGER_BYTES ? 0 : FLUSH_DELAY;
      this.flushTimer = setTimeout(() => {
        this.flushTimer = 0;
        this.flush().catch(error => this.fail(error));
      }, delay);
    }

    // Резолвится, когда все записи, сделанные до вызова, лежат в IndexedDB.
    flush() {
      const result = this.flushChain.then(() => this.drain());
      this.flushChain = result.catch(() => {});
      return result;
    }

    async drain() {
      if (this.flushTimer) { clearTimeout(this.flushTimer); this.flushTimer = 0; }
      const perBatch = Math.max(1, Math.floor(COMMIT_BATCH_BYTES / this.blockSize));
      try {
        while (this.dirty.size) {
          const snapshot = [...this.dirty].sort((a, b) => a - b);
          for (let i = 0; i < snapshot.length; i += perBatch) {
            const chunk = snapshot.slice(i, i + perBatch);
            for (const index of chunk) { this.dirty.delete(index); this.committing.add(index); }
            try {
              await this.store.commitBlocks(this.id, chunk.map(index => [index, this.cache.get(index)]));
              this.diagnostics.committedBytes += chunk.length * this.blockSize;
            } catch (error) {
              for (const index of chunk) this.dirty.add(index); // ничего не теряем, повторим позже
              throw error;
            } finally {
              for (const index of chunk) this.committing.delete(index);
            }
          }
        }
        this.failures = 0;
      } catch (error) {
        if (!this.closed && ++this.failures <= MAX_RETRIES) this.scheduleFlush(RETRY_DELAY);
        throw error;
      }
      this.evict();
    }

    async close() {
      await this.flush();
      this.closed = true;
      if (this.ref) { liveDevices.delete(this.ref); this.ref = null; }
      this.cache.clear(); this.cacheBytes = 0;
    }

    /* --- интерфейс буфера v86 --- */

    get_buffer(callback) { callback(); }
    get_state() { return []; } // диск НЕ входит в save-state: восстанавливать состояние ВМ на изменённый диск нельзя
    set_state() {
      for (const [index, data] of this.cache) {
        if (this.dirty.has(index) || this.committing.has(index)) continue;
        this.cache.delete(index);
        this.cacheBytes -= data.byteLength;
      }
    }
    get_as_file() { throw new Error("Экспорт выполняется через менеджер виртуальных дисков."); }

    fail(error) {
      const normalized = normalizeError(error);
      console.error("Virtual disk I/O failed.", normalized);
      this.onerror?.(normalized);
      window.dispatchEvent(new CustomEvent("osemu-disk-error", { detail: { diskId: this.id, error: normalized } }));
    }

    getDiagnostics() {
      return { ...this.diagnostics, cacheBytes: this.cacheBytes, dirtyBytes: this.dirty.size * this.blockSize, blockSize: this.blockSize, logicalSizeBytes: this.byteLength };
    }
  }

  class IndexedDBMediaDevice extends IndexedDBDiskDevice {
    constructor(store, id) {
      super(store, id, 4 * MB);
      this.media = null;
    }

    async load() {
      this.media = await this.store.getMedia(this.id);
      this.byteLength = this.media.sizeBytes;
      this.blockSize = this.media.blockSize || BLOCK_SIZE;
      this.onload?.({ buffer: null });
    }

    fetchBlocks(indices) { return this.store.readMediaBlocks(this.id, indices); }

    async readRange(offset, length) {
      if (this.media?.blob) return new Uint8Array(await this.media.blob.slice(offset, offset + length).arrayBuffer());
      return super.readRange(offset, length);
    }

    set() { this.fail(new Error("Установочный ISO доступен только для чтения.")); }
  }

  window.OSEmuStorage = {
    DiskStore,
    IndexedDBDiskDevice,
    IndexedDBMediaDevice,
    constants: { blockSize: BLOCK_SIZE, cacheLimit: CACHE_LIMIT, maxIoSize: MAX_IO_SIZE },
  };
})();