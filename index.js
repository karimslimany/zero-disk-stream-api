const express = require('express');
const cors = require('cors');
const torrentStream = require('torrent-stream');

const app = express();
app.use(cors());
app.use(express.json());

// مجلد لحفظ محركات البحث النشطة في الذاكرة
let activeEngines = {};

// شبكة أمان أخيرة للخطأ غير المتوقع
process.on('uncaughtException', (err) => {
    console.error('[uncaughtException] السيرفر استمر بالعمل رغم هذا الخطأ:', err.message);
});
process.on('unhandledRejection', (err) => {
    console.error('[unhandledRejection] السيرفر استمر بالعمل رغم هذا الخطأ:', err && err.message);
});

// 🔥 الحل السحري: دالة إنشاء مخزن "وهمي" يمرر البيانات ويمسحها فوراً لمنع امتلاء الـ RAM والقرص
function createNullStore() {
    let chunks = {};
    return {
        get: (index, cb) => {
            cb(null, chunks[index]);
        },
        put: (index, buf, cb) => {
            chunks[index] = buf;
            cb(null);
            // تدمير القطعة تلقائياً وحذفها من الذاكرة بعد 5 ثوانٍ فقط من استقبالها
            // هذا يضمن تشغيل الفيلم كـ "أنبوب مياه" يمرر البيانات للهاتف ولا يخزنها
            setTimeout(() => {
                delete chunks[index];
            }, 5000);
        },
        close: (cb) => { if (cb) cb(null); },
        destroy: (cb) => { if (cb) cb(null); }
    };
}

// دالة مساعدة لحذف محرك تورنت بأمان مع تنظيف كل مستمعيه
function destroyEngine(infoHash) {
    const engine = activeEngines[infoHash];
    if (!engine) return;
    try { engine.destroy(); } catch (e) { /* تجاهل أي خطأ أثناء الإغلاق */ }
    delete activeEngines[infoHash];
    console.log(`Cleared Engine from RAM for Hash: ${infoHash}`);
}

// 1. استقبال الماغنيت وتفعيل المخزن الصافي (Zero-Disk & Zero-RAM)
app.post('/api/v1/torrents', (req, res) => {
    const { magnet } = req.body || {};
    if (!magnet) return res.status(400).json({ error: "Missing magnet Link" });

    let hashMatch = magnet.match(/btih:([a-zA-Z0-9]{32,40})/i);
    if (!hashMatch) return res.status(400).json({ error: "Invalid magnet hash" });
    let infoHash = hashMatch[1].toLowerCase();

    if (!activeEngines[infoHash]) {
        let engine;
        try {
            // استبدال memoryStore بالمخزن الوهمي المطور لمنع الانهيار نهائياً
            engine = torrentStream(magnet, { storage: createNullStore });
        } catch (err) {
            console.error(`فشل إنشاء محرك التورنت لـ ${infoHash}:`, err.message);
            return res.status(500).json({ error: "Failed to start torrent engine" });
        }

        engine.on('error', (err) => {
            console.error(`خطأ في محرك التورنت لـ ${infoHash}:`, err && err.message);
            destroyEngine(infoHash);
        });

        engine.on('ready', () => {
            activeEngines[infoHash] = engine;
            console.log(`Stateless Torrent Ready: ${engine.torrent.name}`);
        });

        // تنظيف المحرك بالكامل بعد ساعتين من العمل لتفريغ بقايا الذاكرة
        setTimeout(() => destroyEngine(infoHash), 2 * 60 * 60 * 1000);
    }

    res.json({ status: "added", hash: infoHash });
});

// 2. تزويد واجهة الموقع بالبيانات آلياً
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

// 3. البث التدفقي الصافي ودعم تقسيم الحزم (Ranges) لبرامج التحميل
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
app.listen(PORT, () => console.log(`Zero-Disk Streaming API running on port ${PORT}`));
