const express = require('express');
const cors = require('cors');
const torrentStream = require('torrent-stream');
const memoryStore = require('memory-chunk-store');

const app = express();

// أكثر من مصدر مسموح: نطاق الـ Cloudflare Worker، ونطاق السيرفر نفسه على Render
// (هذا الأخير مطلوب لأن فتح الرابط مباشرة داخل متصفح 1DM+ يرسل Origin = نطاق السيرفر نفسه)
const ALLOWED_ORIGINS = [
    "https://karimslimany.workers.dev",
    "https://zero-disk-stream-api-tlad.onrender.com"
];
app.use(cors({
    origin: (origin, callback) => {
        if (!origin || ALLOWED_ORIGINS.includes(origin)) {
            callback(null, true);
        } else {
            // نسجّل القيمة الفعلية المرفوضة حتى نعرف مصدرها بدل تخمينها من اللوغ فقط
            console.warn(`[CORS] طلب مرفوض من Origin غير مسموح: "${origin}"`);
            callback(new Error('Blocked by Security: Unauthorized Origin'));
        }
    }
}));
app.use(express.json());

const APP_SECRET = process.env.APP_SECRET || "Karim_Secure_Streaming_2026_X";

function requireSecret(req, res, next) {
    const provided = req.headers['x-api-token'];
    if (provided !== APP_SECRET) {
        return res.status(401).json({ error: "Unauthorized: Missing or invalid secret token" });
    }
    next();
}

let activeEngines = {};
let engineStatus = {}; // hash -> { state: 'loading'|'ready'|'failed', error?: string, lastAccess?: number }
let pendingQueue = []; // [{ infoHash, magnet, queuedAt }] - طلبات تنتظر دورها لأن MAX_CONCURRENT_ENGINES ممتلئ
let hardFlushCooldownUntil = 0; // بعد تنظيف طارئ، نمنع الطابور من إعادة الملء الفوري ليهدأ الاستخدام الفعلي للذاكرة (GC)

const METADATA_TIMEOUT_MS = 30000; // إذا لم تصل بيانات التورنت خلال 30 ثانية، اعتبره فاشلاً بدل تعليقه للأبد

// عدد التورنتات النشطة المسموح بها في نفس الوقت - كل واحد يخزن بياناته في الـ RAM
// (التخزين المستخدم هو memory-chunk-store، فكل ميجا تُحمَّل تبقى في الذاكرة حتى تدمير المحرك)
// على Render Free Tier (512MB RAM إجمالي)، وميزانية الكاش المحددة بـ 50MB فقط، تورنت واحد نشط
// في نفس الوقت هو الخيار الآمن الوحيد - تورنتان متزامنان قد يتجاوزان الميزانية بسهولة.
const MAX_CONCURRENT_ENGINES = parseInt(process.env.MAX_CONCURRENT_ENGINES || '1', 10);

// سقف الذاكرة "اللين" (RSS بالميجابايت) - إذا تجاوزناه، يُحذف أقدم تورنت لم يُستخدم مؤخراً لتحرير مساحة.
// مضبوط على 130MB ليطابق هدف التشغيل الآمن الإجمالي المحدد لـ Render Free Tier (512MB).
const MAX_MEMORY_MB = parseInt(process.env.MAX_MEMORY_MB || '130', 10);

// سقف طارئ "صارم" - إذا وصلنا هنا فالخطر حقيقي (Render يقتل العملية عند 512MB).
// عند تجاوزه، نحذف كل التورنتات النشطة فوراً بدل الاكتفاء بحذف واحد فقط، لتفادي إعادة تشغيل قسري للسيرفر بالكامل.
const HARD_MEMORY_MB = parseInt(process.env.HARD_MEMORY_MB || '250', 10);

process.on('uncaughtException', (err) => {
    console.error('[uncaughtException] السيرفر مستمر بالعمل:', err.message);
});
process.on('unhandledRejection', (err) => {
    console.error('[unhandledRejection] السيرفر مستمر بالعمل:', err && err.message);
});

function evictLeastRecentlyUsed(excludeHash) {
    let oldestHash = null;
    let oldestTime = Infinity;
    Object.keys(activeEngines).forEach(hash => {
        if (hash === excludeHash) return;
        const t = (engineStatus[hash] && engineStatus[hash].lastAccess) || 0;
        if (t < oldestTime) {
            oldestTime = t;
            oldestHash = hash;
        }
    });
    if (oldestHash) {
        console.log(`[LRU] تحرير الذاكرة: حذف التورنت الأقل استخداماً ${oldestHash}`);
        destroyEngine(oldestHash);
        return true;
    }
    return false;
}

function destroyAllEngines(reason) {
    const hashes = Object.keys(activeEngines);
    console.error(`[HARD LIMIT] ${reason} - تدمير جميع التورنتات النشطة (${hashes.length}) فوراً لتفادي قتل العملية من طرف Render`);
    hardFlushCooldownUntil = Date.now() + 15000; // امنح 15 ثانية للذاكرة كي تهدأ فعلياً قبل قبول طلب جديد من الطابور
    hashes.forEach(hash => destroyEngine(hash));
}

setInterval(() => {
    const memoryUsage = process.memoryUsage();
    const rssMB = memoryUsage.rss / (1024 * 1024);
    console.log(`[Memory Monitor] Total RAM (RSS): ${rssMB.toFixed(2)} MB | Active torrents: ${Object.keys(activeEngines).length}`);

    if (rssMB > HARD_MEMORY_MB) {
        destroyAllEngines(`RSS ${rssMB.toFixed(2)}MB > الحد الطارئ ${HARD_MEMORY_MB}MB`);
    } else if (rssMB > MAX_MEMORY_MB) {
        console.warn(`[Memory Monitor] تجاوزنا الحد الآمن (${MAX_MEMORY_MB} MB) - محاولة تحرير ذاكرة`);
        evictLeastRecentlyUsed();
    } else {
        processQueue(); // تأكيد دوري: استئناف الطابور تلقائياً بعد انتهاء أي فترة تهدئة
    }
}, 10000); // كل 10 ثواني بدل 60 - على بيئة بهذا الضيق (512MB)، الذاكرة قد ترتفع بسرعة أثناء البث

function destroyEngine(infoHash) {
    const engine = activeEngines[infoHash];
    if (!engine) return;
    try { engine.destroy(); } catch (e) { }
    delete activeEngines[infoHash];
    delete engineStatus[infoHash];
    console.log(`Cleared Engine from RAM for Hash: ${infoHash}`);
    processQueue(); // حرّرنا مكاناً - شغّل التالي في الطابور إن وُجد
}

function failEngine(infoHash, reason) {
    const engine = activeEngines[infoHash];
    if (engine) {
        try { engine.destroy(); } catch (e) { }
    }
    delete activeEngines[infoHash];
    engineStatus[infoHash] = { state: 'failed', error: reason };
    console.error(`[FAILED] لم يتم العثور على بيانات/سيدرز لـ ${infoHash}: ${reason}`);
    processQueue(); // حرّرنا مكاناً هنا أيضاً
}

// يبدأ محرك تورنت فعلياً لهذا الـ hash؛ يُستدعى إما مباشرة (يوجد مكان شاغر) أو من الطابور عند تحرر مكان
function startEngine(infoHash, magnet) {
    let engine;
    try {
        // *** الاختلاف الوحيد المتعمد في هذا الاختبار: التخزين الأصلي بدل الكاش المخصص ***
        engine = torrentStream(magnet, { storage: memoryStore });
    } catch (err) {
        console.error(`فشل إنشاء محرك التورنت لـ ${infoHash}:`, err.message);
        engineStatus[infoHash] = { state: 'failed', error: err.message };
        processQueue();
        return;
    }

    engineStatus[infoHash] = { state: 'loading', lastAccess: Date.now() };
    activeEngines[infoHash] = engine; // نخزنه فوراً حتى تظهر حالة "التحميل" في GET

    engine.on('error', (err) => {
        console.error(`[Engine Error] خطأ في محرك التورنت لـ ${infoHash}:`, err && err.message);
        failEngine(infoHash, err && err.message);
    });

    // تسجيل أي نشاط شبكة نراه فعلياً، لمعرفة هل هناك اتصال بالـ swarm أصلاً
    engine.on('torrent', () => console.log(`[DEBUG] تم استقبال بيانات التورنت الأساسية لـ ${infoHash}`));
    engine.swarm && engine.swarm.on && engine.swarm.on('wire', (wire) => {
        console.log(`[DEBUG] اتصال جديد بنظير (peer) لـ ${infoHash}: ${wire.remoteAddress || 'unknown'}`);
    });

    // إذا لم تصل بيانات التورنت (metadata) خلال المهلة، لا تتركه معلقاً - أفشله بوضوح
    const metadataTimer = setTimeout(() => {
        if (engineStatus[infoHash] && engineStatus[infoHash].state === 'loading') {
            failEngine(infoHash, 'No seeders / trackers unreachable within timeout');
        }
    }, METADATA_TIMEOUT_MS);

    engine.on('ready', () => {
        clearTimeout(metadataTimer);
        activeEngines[infoHash] = engine;
        engineStatus[infoHash] = { state: 'ready', lastAccess: Date.now() };

        // مهم لمنع اختناق الذاكرة: بدون هذا، torrent-stream يحمّل كل ملفات التورنت
        // (مثلاً كل حلقات موسم كامل) إلى الـ RAM دفعة واحدة، حتى لو المستخدم يريد ملفاً واحداً فقط
        engine.files.forEach(f => f.deselect());

        console.log(`[SUCCESS] Torrent Ready in RAM: ${engine.torrent.name} (${engine.files.length} ملفات، لا شيء محدد للتحميل بعد)`);
    });

    setTimeout(() => destroyEngine(infoHash), 2 * 60 * 60 * 1000);
}

// يأخذ العنصر التالي من الطابور ويشغّله، بشرط وجود مكان شاغر - بهذا تُعالج طلبات 1DM+ المتعددة بالدور تلقائياً
function processQueue() {
    if (Date.now() < hardFlushCooldownUntil) return; // ما زلنا في فترة التهدئة بعد تنظيف طارئ - لا تبدأ شيئاً جديداً بعد
    while (pendingQueue.length > 0 && Object.keys(activeEngines).length < MAX_CONCURRENT_ENGINES) {
        const next = pendingQueue.shift();
        // تخطَّ أي عنصر أصبح نشطاً بالفعل بطريقة أخرى (احتياط ضد حالات نادرة)
        if (activeEngines[next.infoHash]) continue;
        console.log(`[Queue] بدء تشغيل الطلب التالي من الطابور: ${next.infoHash} (تبقّى ${pendingQueue.length} في الانتظار)`);
        startEngine(next.infoHash, next.magnet);
    }
}

app.post('/api/v1/torrents', requireSecret, (req, res) => {
    const { magnet } = req.body || {};
    if (!magnet) return res.status(400).json({ error: "Missing magnet Link" });

    let hashMatch = magnet.match(/btih:([a-zA-Z0-9]{32,40})/i);
    if (!hashMatch) return res.status(400).json({ error: "Invalid magnet hash" });
    let infoHash = hashMatch[1].toLowerCase();

    console.log(`[DEBUG] بدء إضافة التورنت: ${infoHash}`);

    // موجود مسبقاً (نشط بالفعل) - لا تُعِد إنشاءه، فقط أبلغ حالته الحالية
    if (activeEngines[infoHash]) {
        const status = engineStatus[infoHash] || { state: 'loading' };
        return res.json({ status: status.state, hash: infoHash });
    }

    // موجود مسبقاً في الطابور - أبلغ موقعه بدل تكراره
    if (pendingQueue.some(item => item.infoHash === infoHash)) {
        const pos = pendingQueue.findIndex(item => item.infoHash === infoHash) + 1;
        return res.json({ status: "queued", hash: infoHash, position: pos, queueLength: pendingQueue.length });
    }

    // نضيفه دائماً للطابور أولاً (حتى لو يوجد مكان شاغر) للحفاظ على الترتيب FIFO
    // ثم نعالج الطابور فوراً - إن كان شاغراً سيبدأ هذا الطلب في نفس اللحظة عملياً
    pendingQueue.push({ infoHash, magnet, queuedAt: Date.now() });
    processQueue();

    if (activeEngines[infoHash]) {
        return res.json({ status: "added", hash: infoHash });
    }
    const position = pendingQueue.findIndex(item => item.infoHash === infoHash) + 1;
    console.log(`[Queue] ${infoHash} في الانتظار بالموقع ${position} (الحد ${MAX_CONCURRENT_ENGINES} تورنت متزامن)`);
    res.json({ status: "queued", hash: infoHash, position, queueLength: pendingQueue.length });
});

app.get('/api/v1/torrents', requireSecret, (req, res) => {
    let result = {};

    Object.keys(activeEngines).forEach(hash => {
        const engine = activeEngines[hash];
        const status = engineStatus[hash] || { state: 'loading' };
        result[hash] = {
            Hash: hash,
            Status: status.state,
            Name: engine.torrent ? engine.torrent.name : "Loading Metadata...",
            Files: engine.files ? engine.files.map(f => ({ Path: f.path, Length: f.length })) : []
        };
    });

    // أظهر أيضاً التورنتات التي فشلت، حتى يعرف العميل أنه يجب التوقف عن الانتظار
    Object.keys(engineStatus).forEach(hash => {
        if (engineStatus[hash].state === 'failed' && !result[hash]) {
            result[hash] = {
                Hash: hash,
                Status: 'failed',
                Error: engineStatus[hash].error || 'Unknown error',
                Name: null,
                Files: []
            };
        }
    });

    // أظهر عناصر الطابور مع ترتيب انتظارها - مهم لـ 1DM+ عند جدولة عدة تنزيلات دفعة واحدة
    pendingQueue.forEach((item, index) => {
        result[item.infoHash] = {
            Hash: item.infoHash,
            Status: 'queued',
            Position: index + 1,
            Name: null,
            Files: []
        };
    });

    res.json(result);
});

app.get('/data/*', (req, res) => {
    let filePath;
    try {
        filePath = decodeURIComponent(req.params[0]);
    } catch (err) {
        return res.status(400).send('Invalid file path');
    }

    let targetFile = null;
    let ownerHash = null;
    Object.entries(activeEngines).forEach(([hash, engine]) => {
        if (engine.files) {
            let f = engine.files.find(file => file.path === filePath);
            if (f) { targetFile = f; ownerHash = hash; }
        }
    });

    if (!targetFile) return res.status(404).send('File not found or metadata loading...');

    // حدّث وقت آخر استخدام حتى لا يُحذف هذا التورنت بواسطة نظام تحرير الذاكرة (LRU) أثناء المشاهدة
    if (ownerHash && engineStatus[ownerHash]) {
        engineStatus[ownerHash].lastAccess = Date.now();
    }

    // اختر هذا الملف فقط للتحميل الفعلي؛ هذا يمنع تحميل باقي ملفات التورنت (مثلاً حلقات أخرى) إلى الذاكرة
    targetFile.select();

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
        console.error(`[Timeout] تم قطع الاتصال المعلق لـ "${filePath}".`);
        stream.destroy();
        if (!res.headersSent) res.status(504).send('Torrent stream stalled (No Seeders)');
    }, 30000);

    stream.on('data', () => {
        clearTimeout(streamTimeout);
        streamTimeout = setTimeout(() => stream.destroy(), 30000);
    });

    stream.on('end', () => clearTimeout(streamTimeout));
    stream.on('error', (err) => {
        clearTimeout(streamTimeout);
        failSafely(err);
    });

    stream.pipe(res);
});

app.delete('/api/v1/torrents/:hash', requireSecret, (req, res) => {
    const infoHash = (req.params.hash || '').toLowerCase();

    // إن كان نشطاً - دمّره (يحرر مكاناً ويشغّل تلقائياً التالي من الطابور عبر destroyEngine)
    if (activeEngines[infoHash]) {
        destroyEngine(infoHash);
        return res.json({ status: 'cancelled', hash: infoHash });
    }

    // إن كان في الطابور فقط - احذفه من مكانه بدون التأثير على البقية
    const queueIndex = pendingQueue.findIndex(item => item.infoHash === infoHash);
    if (queueIndex !== -1) {
        pendingQueue.splice(queueIndex, 1);
        return res.json({ status: 'cancelled', hash: infoHash });
    }

    res.status(404).json({ error: 'Not found in active engines or queue' });
});

// معالج أخطاء صريح - يمنع Express من إرسال صفحة HTML بها stack trace كامل للعميل عند رفض CORS أو أي خطأ آخر
app.use((err, req, res, next) => {
    if (err && err.message === 'Blocked by Security: Unauthorized Origin') {
        return res.status(403).json({ error: 'Unauthorized Origin' });
    }
    console.error('[Unhandled Error]', err && err.message);
    if (!res.headersSent) res.status(500).json({ error: 'Internal server error' });
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => console.log(`Diagnostic API running on port ${PORT}`));
