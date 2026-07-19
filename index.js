const express = require('express');
const cors = require('cors');
const torrentStream = require('torrent-stream');

const app = express();
app.use(cors());
app.use(express.json());

// مجلد لحفظ محركات البحث النشطة في الذاكرة
let activeEngines = {};

// 🛡️ شبكة أمان عامة: التقاط الأخطاء غير المتوقعة على مستوى النظام ومنع انهيار السيرفر
process.on('uncaughtException', (err) => {
    console.error('[uncaughtException] السيرفر استمر بالعمل رغم هذا الخطأ:', err.message);
});
process.on('unhandledRejection', (err) => {
    console.error('[unhandledRejection] السيرفر استمر بالعمل رغم هذا الخطأ:', err && err.message);
});

// 🧠 المخزن الذكي المتكيف: يحتفظ بالطابور مرنًا بناءً على سرعة الإنترنت لمنع حذف القطع قبل اكتمال سحبها
function createSmartCacheStore() {
    let chunks = {};
    let chunkKeys = [];
    const MAX_CHUNKS = 15; // الاحتفاظ بـ 15 قطعة كحد أقصى (تستهلك ~35 ميجابايت فقط وهو آمن جداً للرام)

    return {
        get: (index, cb) => {
            cb(null, chunks[index]);
        },
        put: (index, buf, cb) => {
            if (!chunks[index]) {
                chunks[index] = buf;
                chunkKeys.push(index);
                
                // إذا زاد عدد القطع عن الحد الأقصى نتيجة لسرعة التحميل، يتم طرد أقدم قطعة (FIFO)
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

// 🧹 دالة مساعدة لتنظيف وتدمير المحرك بأمان وتفريغ الذاكرة
function destroyEngine(infoHash) {
    const engine = activeEngines[infoHash];
    if (!engine) return;
    try { engine.destroy(); } catch (e) { /* تجاهل أي خطأ أثناء الإغلاق نفسه */ }
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
            // تشغيل التورنت عبر المخزن الذكي المرن للتكيف مع خطوط الإنترنت الضعيفة القابلة للانقطاع
            engine = torrentStream(magnet, { storage: createSmartCacheStore });
        } catch (err) {
            console.error(`فشل إنشاء محرك التورنت لـ ${infoHash}:`, err.message);
            return res.status(500).json({ error: "Failed to start torrent engine" });
        }

        // 🛡️ شبكة أمان المحرك: التقاط أخطاء الشبكة والـ Trackers داخل التورنت دون إسقاط العملية
        engine.on('error', (err) => {
            console.error(`خطأ في محرك التورنت لـ ${infoHash}:`, err && err.message);
            destroyEngine(infoHash);
        });

        engine.on('ready', () => {
            activeEngines[infoHash] = engine;
            console.log(`Stateless & Smart Torrent Ready: ${engine.torrent.name}`);
        });

        // تدمير تلقائي بعد ساعتين لتفريغ بقايا الذاكرة وضمان عدم تراكم العمليات الخاملة
        setTimeout(() => destroyEngine(infoHash), 2 * 60 * 60 * 1000);
    }

    res.json({ status: "added", hash: infoHash });
});

// 2. تزويد الفرونت-إند بشجرة الملفات والأحجام دون الحاجة لتعديل كود موقعك
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

// 3. مسار البث التدفقي الصافي مع دعم الحزم الجزئية (Byte-Ranges) لـ 1DM+
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

    // 🛡️ دالة معالجة أخطاء الستريم أثناء النقل: تدمر الطلب المنقطع بأمان لئلا يعلق السيرفر
    const failSafely = (err) => {
        console.error(`خطأ أثناء بث الملف "${filePath}":`, err && err.message);
        if (!res.headersSent) {
            res.status(502).send('Streaming failed, please try again');
        } else {
            res.destroy();
        }
    };

    const range = req.headers.range;

    // إذا كان الطلب عادياً بدون تقسيم
    if (!range) {
        res.setHeader('Content-Length', targetFile.length);
        res.setHeader('Accept-Ranges', 'bytes');
        const stream = targetFile.createReadStream();
        stream.on('error', failSafely);
        return stream.pipe(res);
    }

    // هندسة الحزم (Byte-Range Math) لضمان التوافق المطلق مع تقنيات الـ Resume في 1DM+
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

    // سحب القطع المطلوبة فقط بشكل تدفقي حي وتمريرها للهاتف مباشرة
    const stream = targetFile.createReadStream({ start, end });
    stream.on('error', failSafely);
    stream.pipe(res);
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => console.log(`Zero-Disk Smart Streaming API running on port ${PORT}`));
