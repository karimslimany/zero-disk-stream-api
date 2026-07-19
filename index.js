const express = require('express');
const cors = require('cors');
const torrentStream = require('torrent-stream');

const app = express();

// 1. [تحسين مضاف]: قفل الحماية وتقييد الـ CORS على موقعك فقط لمنع الاستغلال الخارجي
const ALLOWED_ORIGIN = "https://karimslimany.workers.dev"; // ضع دومين موقعك هنا بدقة

app.use(cors({
    origin: (origin, callback) => {
        // السماح بالطلبات التي لا تحتوي على Origin (مثل تطبيقات الـ Android كـ 1DM+ أو أداة UptimeRobot)
        if (!origin || origin === ALLOWED_ORIGIN) {
            callback(null, true);
        } else {
            callback(new Error('Blocked by Security: Unauthorized Origin'));
        }
    }
}));
app.use(express.json());

let activeEngines = {};

// شبكة أمان عامة للخطأ غير المتوقع
process.on('uncaughtException', (err) => {
    console.error('[uncaughtException] السيرفر مستمر بالعمل:', err.message);
});
process.on('unhandledRejection', (err) => {
    console.error('[unhandledRejection] السيرفر مستمر بالعمل:', err && err.message);
});

// 🧠 [التعديل الجوهري]: كاش ذكي يعتمد على ميزانية البايتات الفعلية (~300MB) لتفادي التلف الصامت
function createByteLimitStore() {
    let chunks = {};
    let chunkKeys = [];
    let currentCacheBytes = 0;
    const MAX_CACHE_BYTES = 300 * 1024 * 1024; // 🚀 ميزانية صارمة: 300 ميجابايت كحد أقصى

    return {
        get: (index, cb) => {
            cb(null, chunks[index]);
        },
        put: (index, buf, cb) => {
            if (!chunks[index]) {
                chunks[index] = buf;
                chunkKeys.push(index);
                currentCacheBytes += buf.length;

                // التخلص الذكي من أقدم القطع بالتتابع فقط عندما نتجاوز حاجز الـ 300 ميجابايت
                while (currentCacheBytes > MAX_CACHE_BYTES && chunkKeys.length > 0) {
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

// 📈 [تحسين مضاف]: فحص وتمرير تقرير استهلاك الرام الحقيقي (RSS) إلى الـ Logs كل دقيقة للمراقبة الحية
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

// 1. استقبال روابط الماغنيت
app.post('/api/v1/torrents', (req, res) => {
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
            console.log(`Byte-Limit Adaptive Torrent Ready: ${engine.torrent.name}`);
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

// 3. مسار البث التدفقي الصافي لـ 1DM+ مع مهلة انتظار ذكية لقفل التدفق المعلق
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

    // ⏱️ [تحسين مضاف]: تدمير الستريم المفتوح فوراً إذا انقطع تدفق البيانات تماماً (No Seeders) لمدة 30 ثانية
    let streamTimeout = setTimeout(() => {
        console.error(`[Timeout] تم قطع الاتصال المعلق لـ "${filePath}" بسبب توقف تدفق البيانات من شبكة التورنت.`);
        stream.destroy();
        if (!res.headersSent) res.status(504).send('Torrent stream stalled (No Seeders)');
    }, 30000);

    stream.on('data', () => {
        // إنعاش المؤقت الزمني مع كل حزمة بايتات جديدة تصل بنجاح
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
app.listen(PORT, () => console.log(`Zero-Disk Byte-Limit API running on port ${PORT}`));
