/*
    minifs (Fleen5177) beta 1.8
    Original logic by Igor2677 / playerony
*/
const TYPE = { DIR: 'directory', FILE: 'file' };
const PFX = 'TSSSFILE>>>';

const pathUtil = {
    norm: p => p.replace(/\\/g, '/').replace(/\/+/g, '/'),
    join: (...p) => pathUtil.norm(p.join('/')),
    base: p => p.split('/').pop(),
    dir: p => p.split('/').slice(0, -1).join('/') || '.'
};

const enc = {
    txt: s => PFX + btoa(unescape(encodeURIComponent(s))),
    decTxt: d => d.startsWith(PFX) ? decodeURIComponent(escape(atob(d.slice(11)))) : d,
    bin: b => PFX + b,
    decBin: d => d.startsWith(PFX) ? 'data:text/plain;base64,' + d.slice(11) : d
};

const createFs = (props = {}) => {
    const cfg = { databaseName: 'indexeddb-fs', databaseVersion: 1, objectStoreName: 'files', rootDirectoryName: 'root', ...props };
    const root = cfg.rootDirectoryName;
    let _db;

    const dbOp = (mode, fn) => new Promise((res, rej) => {
        const req = indexedDB.open(cfg.databaseName, cfg.databaseVersion);
        req.onerror = () => rej(req.error);
        req.onupgradeneeded = e => {
            const d = e.target.result;
            if (!d.objectStoreNames.contains(cfg.objectStoreName)) 
                d.createObjectStore(cfg.objectStoreName, { keyPath: 'fullPath' }).createIndex('directory', 'directory');
        };
        req.onsuccess = () => {
            _db = req.result;
            const tx = _db.transaction(cfg.objectStoreName, mode).objectStore(cfg.objectStoreName);
            const r = fn(tx);
            r.onsuccess = () => res(r.result);
            r.onerror = () => rej(r.error);
        };
    });

    const get = k => dbOp('readonly', s => s.get(k));
    const put = v => dbOp('readwrite', s => s.put(v));
    const del = k => dbOp('readwrite', s => s.delete(k));
    const idx = k => dbOp('readonly', s => s.index('directory').getAll(k));

    const resolve = p => {
        if (!p) throw new Error('Path required');
        let c = pathUtil.norm(p);
        if (c === '/' || c === '' || c === '.') return root;
        if (c.startsWith(root + '/') || c === root) return c;
        return root + '/' + (c.startsWith('/') ? c.slice(1) : c);
    };

    const check = (cond, msg) => { if (cond) throw new Error(msg); };
    const ensureRoot = async () => !(await get(root)) && put({ fullPath: root, name: root, directory: root, type: TYPE.DIR, isRoot: true, createdAt: Date.now() });
    
    const api = (fn) => async (...args) => { await ensureRoot(); return fn(...args); };

    return {
        databaseName: cfg.databaseName,
        databaseVersion: cfg.databaseVersion,
        objectStoreName: cfg.objectStoreName,
        rootDirectoryName: root,
        
        exists: api(async p => !!(await get(resolve(p)))),
        
        isFile: api(async p => (await get(resolve(p)))?.type === TYPE.FILE),
        
        isDirectory: api(async p => (await get(resolve(p)))?.type === TYPE.DIR),
        
        createDirectory: api(async p => {
            const fp = resolve(p), dp = pathUtil.dir(fp);
            if (fp === root) throw new Error("Root exists");
            const parent = await get(dp);
            check(!parent || parent.type !== TYPE.DIR, `Parent "${dp}" missing`);
            const ex = await get(fp);
            if (ex) { check(ex.type === TYPE.FILE, `"${fp}" is file`); return ex; }
            const d = { fullPath: fp, name: pathUtil.base(fp), directory: dp, type: TYPE.DIR, isRoot: false, createdAt: Date.now() };
            await put(d); return d;
        }),

        readDirectory: api(async p => {
            const fp = resolve(p);
            check((await get(fp))?.type !== TYPE.DIR, `"${fp}" not dir`);
            const all = await idx(fp), files = [], dirs = [];
            all.forEach(x => (x.type === TYPE.FILE ? files : dirs).push(x));
            return { isEmpty: !files.length && !dirs.length, filesCount: files.length, directoriesCount: dirs.length, files: files.map(({data, ...r})=>r), directories: dirs };
        }),

        writeFile: api(async (p, c) => {
            const fp = resolve(p);
            if(fp === root) throw new Error("Root write deny");
            const dp = pathUtil.dir(fp);
            const dir = await get(dp);
            check(!dir || dir.type !== TYPE.DIR, `Dir "${dp}" missing`);
            check((await get(fp))?.type === TYPE.DIR, `"${fp}" is dir`);
            const rec = { fullPath: fp, name: pathUtil.base(fp), directory: dp, type: TYPE.FILE, data: enc.txt(c), createdAt: Date.now() };
            await put(rec); return { ...rec, data: undefined };
        }),

        writeFileBin: api(async (p, b) => {
            const fp = resolve(p), dp = pathUtil.dir(fp);
            if(fp === root) throw new Error("Root write deny");
            check((await get(dp))?.type !== TYPE.DIR, `Dir "${dp}" missing`);
            await put({ fullPath: fp, name: pathUtil.base(fp), directory: dp, type: TYPE.FILE, data: enc.bin(b), createdAt: Date.now() });
        }),

        readFile: api(async p => {
            const r = await get(resolve(p));
            check(!r || r.type !== TYPE.FILE, `File "${p}" missing`);
            return enc.decTxt(r.data);
        }),

        readFileBin: api(async p => {
            const r = await get(resolve(p));
            check(!r || r.type !== TYPE.FILE, `File "${p}" missing`);
            return enc.decBin(r.data);
        }),

        removeFile: api(async p => {
            const fp = resolve(p);
            check((await get(fp))?.type !== TYPE.FILE, `"${fp}" not file`);
            await del(fp);
        }),

        removeDirectory: api(async p => {
            const fp = resolve(p);
            check((await get(fp))?.type !== TYPE.DIR, `"${fp}" not dir`);
            const recDel = async t => {
                const c = await idx(t);
                for(const i of c) i.type === TYPE.DIR ? await recDel(i.fullPath) : await del(i.fullPath);
                if(t !== root) await del(t);
            };
            await recDel(fp);
        }),

        renameFile: api(async (old, name) => {
            const fp = resolve(old), rec = await get(fp);
            check(!rec || rec.type !== TYPE.FILE, "Not file");
            const newFp = pathUtil.join(pathUtil.dir(fp), name);
            check(await get(newFp), "Exists");
            await put({ ...rec, name, fullPath: newFp }); await del(fp);
            return { ...rec, name, fullPath: newFp };
        }),

        moveFile: api(async (src, dst) => {
            const s = resolve(src), d = resolve(dst), rec = await get(s);
            check(!rec || rec.type !== TYPE.FILE, "Src not file");
            const dd = pathUtil.dir(d);
            check((await get(dd))?.type !== TYPE.DIR, "Dst dir missing");
            check(await get(d), "Dst exists");
            const u = { ...rec, fullPath: d, name: pathUtil.base(d), directory: dd };
            await put(u); await del(s); return u;
        }),

        copyFile: async (s, d) => { try { await api(async()=>0)(); const b = (await get(resolve(s))).data; await put({ fullPath: resolve(d), name: pathUtil.base(resolve(d)), directory: pathUtil.dir(resolve(d)), type: TYPE.FILE, data: b, createdAt: Date.now() }); } catch(e){} },
        
        copyDirectory: async (s, d) => {
            try {
                await api(async()=>0)();
                const copy = async (src, dst) => {
                    const l = await idx(src);
                    await put({ fullPath: dst, name: pathUtil.base(dst), directory: pathUtil.dir(dst), type: TYPE.DIR, isRoot: false, createdAt: Date.now() });
                    for(const i of l) {
                        const nDst = dst + '/' + i.name;
                        if(i.type === TYPE.FILE) await put({ ...i, fullPath: nDst, directory: dst, createdAt: Date.now() });
                        else await copy(i.fullPath, nDst);
                    }
                };
                await copy(resolve(s), resolve(d));
            } catch(e){}
        },

        details: api(async p => { const r = await get(resolve(p)); if(!r) throw new Error("Missing"); return r; }),
        fileDetails: api(async p => { const r = await get(resolve(p)); check(r?.type!==TYPE.FILE, "Not file"); return r; }),
        directoryDetails: api(async p => { const r = await get(resolve(p)); check(r?.type!==TYPE.DIR, "Not dir"); return r; }),
        remove: api(async p => (await get(resolve(p)))?.type === TYPE.DIR ? await fs.removeDirectory(p) : await del(resolve(p))) 
    };
};
const fs = createFs();
