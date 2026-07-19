const express = require('express');
const cors = require('cors');
const torrentStream = require('torrent-stream');

const app = express();
app.use(cors());
app.use(express.json());

// مجلد لحفظ محركات البحث النشطة في الذاكرة
let activeEngines = {};

// 🛡️ شبكة أمان عامة للخطأ غير المتوقع
process.on('uncaughtException', (err) => {
    console.error('[uncaughtException] السيرفر استمر بالعمل رغم هذا الخطأ:', err.message);
});
process.on('unhandledRejection', (err) => {
    console.error('[unhandledRejection] السيرفر استمر بالعمل رغم هذا الخطأ:', err && err.message);
});

// 🧠 الكاش الميكروي الصارم: حماية مطلقة للرام من الانفجار مهما عظم حجم الملف
function createMicroCacheStore() {
    let chunks = {};
    let chunkKeys = [];
    const MAX_CHUNKS = 4; // 🚀 4 قطع فقط كحد أقصى! تضمن بقاء استهلاك الرام تحت حاجز 30 ميجابايت دائماً

    return {
        get: (index, cb) => {
            cb(null, chunks[index]);
        },
        put: (index, buf, cb) => {
            if (!chunks[index]) {
                chunks[index] = buf;
                chunkKeys.push(index);
                
                // طرد فوري لأي قطعة قديمة بمجرد دخول قطعة جديدة ليبقى الاستهلاك ثابتاً
                if (chunkKeys.length > MAX_CHUNKS) {
                    let oldestIndex = chunkKeys.shift();
                    delete chunks[oldestIndex];
                }
            }
            cb(null);
        },
        close: (cb) => { if (cb) cb(null); },
        destroy: (cb) => { if (cb) cb(null); }
    };
}

function destroyEngine(infoHash) {
    const engine = activeEngines[infoHash];
    if (!engine) return;
    try { engine.destroy(); } catch (e) { }
    delete activeEngines[infoHash];
    console.log(`Cleared Engine from RAM for Hash: ${infoHash}`);
}

// 1. استقبال روابط الماغنيت وتفعيل البث عديم الحالة (Stateless)
app.post('/api/v1/torrents', (req, res) => {
    const { magnet } = req.body || {};
    if (!magnet) return res.status(400).json({ error: "Missing magnet Link" });

    let hashMatch = magnet.match(/btih:([a-zA-Z0-9]{32,40})/i);
    if (!hashMatch) return res.status(400).json({ error: "Invalid magnet hash" });
    let infoHash = hashMatch[1].toLowerCase();

    if (!activeEngines[infoHash]) {
        let engine;
        try {
            engine = torrentStream(magnet, { storage: createMicroCacheStore });
        } catch (err) {
            console.error(`فشل إنشاء محرك التورنت لـ ${infoHash}:`, err.message);
            return res.status(500).json({ error: "Failed to start torrent engine" });
        }

        engine.on('error', (err) => {
            console.error(`[Warning] خطأ قراءة في محرك التورنت لـ ${infoHash}:`, err && err.message);
        });

        engine.on('ready', () => {
            activeEngines[infoHash] = engine;
            console.log(`Micro-Cache Torrent Ready: ${engine.torrent.name}`);
        });

        setTimeout(() => destroyEngine(infoHash), 2 * 60 * 60 * 1000);
    }

    res.json({ status: "added", hash: infoHash });
});

// 2. تزويد الفرونت-إند بشجرة الملفات
app.get('/api/v1/torrents', (req, res) => {
    let result = {};
    Object.keys(activeEngines).forEach(hash => {
        const engine = activeEngines[hash];
        result[hash] = {
            Hash: hash,
            Name: engine.torrent ? engine.torrent.name : "Loading Metadata...",
            Files: engine.files ? engine.files.map(f => ({ Path: f.path, Length: f.length })) : []
        };
    });
    res.json(result);
});

// 3. مسار البث التدفقي الصافي لـ 1DM+
app.get('/data/*', (req, res) => {
    let filePath;
    try {
        filePath = decodeURIComponent(req.params[0]);
    } catch (err) {
        return res.status(400).send('Invalid file path');
    }

    let targetFile = null;
    Object.values(activeEngines).forEach(engine => {
        if (engine.files) {
            let f = engine.files.find(file => file.path === filePath);
            if (f) targetFile = f;
        }
    });

    if (!targetFile) return res.status(404).send('File not found or metadata loading...');

    const failSafely = (err) => {
        console.error(`خطأ أثناء بث الملف "${filePath}":`, err && err.message);
        if (!res.headersSent) {
            res.status(502).send('Streaming failed, please try again');
        } else {
            res.destroy();
        }
    };

    const range = req.headers.range;

    if (!range) {
        res.setHeader('Content-Length', targetFile.length);
        res.setHeader('Accept-Ranges', 'bytes');
        const stream = targetFile.createReadStream();
        stream.on('error', failSafely);
        return stream.pipe(res);
    }

    const parts = range.replace(/bytes=/, "").split("-");
    const start = parseInt(parts[0], 10);
    const end = parts[1] ? parseInt(parts[1], 10) : targetFile.length - 1;

    if (isNaN(start) || isNaN(end) || start > end || end >= targetFile.length) {
        return res.status(416).send('Invalid range');
    }

    const chunksize = (end - start) + 1;

    res.writeHead(206, {
        'Content-Range': `bytes ${start}-${end}/${targetFile.length}`,
        'Accept-Ranges': 'bytes',
        'Content-Length': chunksize,
        'Content-Type': 'video/mp4'
    });

    const stream = targetFile.createReadStream({ start, end });
    stream.on('error', failSafely);
    stream.pipe(res);
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => console.log(`Micro-Cache Streaming API running on port ${PORT}`));
