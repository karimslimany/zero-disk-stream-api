const express = require('express');
const cors = require('cors');
const torrentStream = require('torrent-stream');

const app = express();

// 🔐 التوكن السري لحماية السيرفر من الاستغلال الخارجي
const API_SECRET_TOKEN = process.env.API_SECRET_TOKEN || "Karim_Secure_Streaming_2026_X";

app.use(cors()); 
app.use(express.json());

let activeEngines = {};

// شبكة أمان عامة للخطأ غير المتوقع
process.on('uncaughtException', (err) => {
    console.error('[uncaughtException] السيرفر مستمر بالعمل:', err.message);
});
process.on('unhandledRejection', (err) => {
    console.error('[unhandledRejection] السيرفر مستمر بالعمل:', err && err.message);
});

// 🧠 كاش البايتات الميكروي الآمن: ميزانية محكمة لحماية الرام من الانفجار نهائياً
function createByteLimitStore() {
    let chunks = {};
    let chunkKeys = [];
    let currentCacheBytes = 0;
    const MAX_CACHE_BYTES = 50 * 1024 * 1024; // ميزانية 50 ميجابايت (آمنة تماماً ومثالية للمسار الواحد)

    return {
        get: (index, cb) => { cb(null, chunks[index]); },
        put: (index, buf, cb) => {
            if (!chunks[index]) {
                chunks[index] = buf;
                chunkKeys.push(index);
                currentCacheBytes += buf.length;

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

// 📈 مراقبة استهلاك الرام الحقيقي (RSS) في الـ Logs كل دقيقة
setInterval(() => {
    const memoryUsage = process.memoryUsage();
    const rssMB = (memoryUsage.rss / (1024 * 1024)).toFixed(2);
    console.log(`[Memory Monitor] Total RAM (RSS): ${rssMB} MB`);
}, 60000);

function destroyEngine(infoHash) {
    const engine = activeEngines[infoHash];
    if (!engine) return;
    try { engine.destroy(); } catch (e) { }
    delete activeEngines[infoHash];
    console.log(`Cleared Engine from RAM for Hash: ${infoHash}`);
}

// مسار فحص الحيوية (Health Check) مخصص لـ UptimeRobot
app.get('/ping', (req, res) => {
    res.status(200).send('pong');
});

// 1. استقبال روابط الماغنيت وحقن التراكرز المسموحة
app.post('/api/v1/torrents', (req, res) => {
    const clientToken = req.headers['x-api-token'];
    if (!clientToken || clientToken !== API_SECRET_TOKEN) {
        return res.status(401).json({ error: "Unauthorized" });
    }

    const { magnet } = req.body || {};
    if (!magnet) return res.status(400).json({ error: "Missing magnet Link" });

    let hashMatch = magnet.match(/btih:([a-zA-Z0-9]{32,40})/i);
    if (!hashMatch) return res.status(400).json({ error: "Invalid magnet hash" });
    let infoHash = hashMatch[1].toLowerCase();

    if (!activeEngines[infoHash]) {
        // 🚀 [تحسين مضاف]: قائمة تراكرز مخصصة تعمل ببروتوكول HTTP/TCP المفتوح لتخطي حظر ريندر للـ UDP
        const httpTrackers = [
            "http://tracker.gbitt.info:80/announce",
            "http://tracker.nyap2p.com:8080/announce",
            "https://tracker.nanoha.org:443/announce",
            "http://share.tracker.v2ph.com:80/announce",
            "http://tracker.files.fm:6969/announce",
            "http://open.acgnxtracker.com:80/announce"
        ];
        
        let enhancedMagnet = magnet;
        httpTrackers.forEach(tr => {
            if (!enhancedMagnet.includes(encodeURIComponent(tr)) && !enhancedMagnet.includes(tr)) {
                enhancedMagnet += `&tr=${encodeURIComponent(tr)}`;
            }
        });

        let engine;
        try {
            engine = torrentStream(enhancedMagnet, { storage: createByteLimitStore });
            
            // 🚀 [تعديل جوهري]: تسجيل المحرك في الذاكرة فوراً لكي يراه الفرونت-إند ويعرف أنه قيد التحضير
            activeEngines[infoHash] = engine;
        } catch (err) {
            console.error(`فشل إنشاء محرك التورنت لـ ${infoHash}:`, err.message);
            return res.status(500).json({ error: "Failed to start torrent engine" });
        }

        engine.on('error', (err) => {
            console.error(`[Warning] خطأ في محرك التورنت لـ ${infoHash}:`, err && err.message);
        });

        engine.on('ready', () => {
            console.log(`Byte-Limit Adaptive Torrent Ready: ${engine.torrent.name}`);
        });

        setTimeout(() => destroyEngine(infoHash), 2 * 60 * 60 * 1000);
    }

    res.json({ status: "added", hash: infoHash });
});

// 2. تزويد الفرونت-إند بشجرة الملفات
app.get('/api/v1/torrents', (req, res) => {
    const clientToken = req.headers['x-api-token'];
    if (!clientToken || clientToken !== API_SECRET_TOKEN) {
        return res.status(401).json({ error: "Unauthorized" });
    }

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
        try { res.destroy(); } catch (e) {}
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
        console.error(`[Timeout Check] Stalled stream destroyed for: "${filePath}"`);
        try { stream.destroy(); res.destroy(); } catch (e) {}
    }, 30000);

    stream.on('data', () => {
        clearTimeout(streamTimeout);
        streamTimeout = setTimeout(() => {
            try { stream.destroy(); res.destroy(); } catch (e) {}
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
app.listen(PORT, () => console.log(`Zero-Disk Secure Byte-Limit API running on port ${PORT}`));
