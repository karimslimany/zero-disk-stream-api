const express = require('express');
const cors = require('cors');
const torrentStream = require('torrent-stream');

const app = express();

// قفل CORS على موقعك فقط (طبقة أولى، شكلية أكثر منها حماية فعلية)
const ALLOWED_ORIGIN = "https://karimslimany.workers.dev";
app.use(cors({
    origin: (origin, callback) => {
        if (!origin || origin === ALLOWED_ORIGIN) {
            callback(null, true);
        } else {
            callback(new Error('Blocked by Security: Unauthorized Origin'));
        }
    }
}));
app.use(express.json());

// 🔐 الحماية الحقيقية: توكن سري يجب أن يصل مع كل طلب حساس. يُحقن هذا التوكن
// تلقائياً من داخل الـ Worker (ولا يظهر أبداً في كود الموقع المرئي). القيمة
// هنا مطابقة تماماً لما ضبطته في الـ Worker — الأفضل ضبطها كمتغير بيئة
// باسم APP_SECRET على Render بدل تركها مكتوبة بالكود مباشرة
const APP_SECRET = process.env.APP_SECRET || "Karim_Secure_Streaming_2026_X";

function requireSecret(req, res, next) {
    const provided = req.headers['x-api-token'];
    if (provided !== APP_SECRET) {
        return res.status(401).json({ error: "Unauthorized: Missing or invalid secret token" });
    }
    next();
}

let activeEngines = {};

// 🛡️ شبكة أمان عامة للخطأ غير المتوقع — تمنع انهيار السيرفر بالكامل
process.on('uncaughtException', (err) => {
    console.error('[uncaughtException] السيرفر مستمر بالعمل:', err.message);
});
process.on('unhandledRejection', (err) => {
    console.error('[unhandledRejection] السيرفر مستمر بالعمل:', err && err.message);
});

// 🧠 كاش ميكروي يعتمد على ميزانية بايتات صارمة (50MB) لضمان ثبات الرام
// الإجمالي للسيرفر ضمن حدود Render المجاني (512MB) بهامش أمان كبير
const MAX_CACHE_BYTES = 50 * 1024 * 1024;

function createByteLimitStore() {
    let chunks = {};
    let chunkKeys = [];
    let currentCacheBytes = 0;

    return {
        get: (index, cb) => {
            cb(null, chunks[index]);
        },
        put: (index, buf, cb) => {
            if (!chunks[index]) {
                chunks[index] = buf;
                chunkKeys.push(index);
                currentCacheBytes += buf.length;

                while (currentCacheBytes > MAX_CACHE_BYTES && chunkKeys.length > 1) {
                    let oldestIndex = chunkKeys.shift();
                    if (chunks[oldestIndex]) {
                        currentCacheBytes -= chunks[oldestIndex].length;
                        delete chunks[oldestIndex];
                    }
                }
            }
            cb(null);
        },
        close: (cb) => { if (cb) cb(null); },
        destroy: (cb) => { if (cb) cb(null); }
    };
}

// 📈 تقرير استهلاك الرام الحقيقي (RSS) إلى الـ Logs كل دقيقة للمراقبة الحية
setInterval(() => {
    const memoryUsage = process.memoryUsage();
    const rssMB = (memoryUsage.rss / (1024 * 1024)).toFixed(2);
    const heapUsedMB = (memoryUsage.heapUsed / (1024 * 1024)).toFixed(2);
    console.log(`[Memory Monitor] Total RAM (RSS): ${rssMB} MB | Active Heap: ${heapUsedMB} MB`);
}, 60000);

function destroyEngine(infoHash) {
    const engine = activeEngines[infoHash];
    if (!engine) return;
    try { engine.destroy(); } catch (e) { }
    delete activeEngines[infoHash];
    console.log(`Cleared Engine from RAM for Hash: ${infoHash}`);
}

// 1. استقبال روابط الماغنيت — محمي بالتوكن السري
app.post('/api/v1/torrents', requireSecret, (req, res) => {
    const { magnet } = req.body || {};
    if (!magnet) return res.status(400).json({ error: "Missing magnet Link" });

    let hashMatch = magnet.match(/btih:([a-zA-Z0-9]{32,40})/i);
    if (!hashMatch) return res.status(400).json({ error: "Invalid magnet hash" });
    let infoHash = hashMatch[1].toLowerCase();

    if (!activeEngines[infoHash]) {
        let engine;
        try {
            engine = torrentStream(magnet, { storage: createByteLimitStore });
        } catch (err) {
            console.error(`فشل إنشاء محرك التورنت لـ ${infoHash}:`, err.message);
            return res.status(500).json({ error: "Failed to start torrent engine" });
        }

        engine.on('error', (err) => {
            console.error(`[Warning] خطأ قراءة في محرك التورنت لـ ${infoHash}:`, err && err.message);
        });

        engine.on('ready', () => {
            activeEngines[infoHash] = engine;
            console.log(`Torrent Ready in RAM: ${engine.torrent.name}`);
        });

        setTimeout(() => destroyEngine(infoHash), 2 * 60 * 60 * 1000);
    }

    res.json({ status: "added", hash: infoHash });
});

// 2. تزويد واجهة موقعك بشجرة الملفات — محمي بالتوكن السري أيضاً
app.get('/api/v1/torrents', requireSecret, (req, res) => {
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

// 3. البث التدفقي الصافي — بدون توكن عمداً، لأن 1DM+ يفتح هذا الرابط مباشرة
// بدون أي ترويسات مخصصة، فلا يمكن إرفاق التوكن معه إطلاقاً
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

    let streamTimeout = setTimeout(() => {
        console.error(`[Timeout] تم قطع الاتصال المعلق لـ "${filePath}" بسبب توقف تدفق البيانات من شبكة التورنت.`);
        stream.destroy();
        if (!res.headersSent) res.status(504).send('Torrent stream stalled (No Seeders)');
    }, 30000);

    stream.on('data', () => {
        clearTimeout(streamTimeout);
        streamTimeout = setTimeout(() => {
            console.error(`[Timeout] انقطع التدفق فجأة أثناء التحميل لـ "${filePath}".`);
            stream.destroy();
        }, 30000);
    });

    stream.on('end', () => clearTimeout(streamTimeout));
    stream.on('error', (err) => {
        clearTimeout(streamTimeout);
        failSafely(err);
    });

    stream.pipe(res);
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => console.log(`Zero-Disk Byte-Limit Secured API running on port ${PORT}`));
