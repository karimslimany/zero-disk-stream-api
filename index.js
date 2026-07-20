const express = require('express');
const cors = require('cors');
const path = require('path');
const torrentStream = require('torrent-stream');
const slidingWindowStore = require('./sliding-window-store');

// خريطة امتداد الملف -> Content-Type الصحيح. الكود الأصلي كان يفرض 'video/mp4' دائماً
// بغض النظر عن الامتداد الفعلي (وحتى بدون Content-Type إطلاقاً في مسار عدم وجود Range) -
// وهذا يكسر التشغيل الصحيح في بعض المشغلات لملفات mkv/avi/webm إلخ، لأنها تعتمد على
// الـ Content-Type لاختيار الـ demuxer/codec المناسب بدل تخمينه من محتوى الملف.
const MIME_TYPES = {
    '.mp4': 'video/mp4',
    '.m4v': 'video/mp4',
    '.mkv': 'video/x-matroska',
    '.webm': 'video/webm',
    '.avi': 'video/x-msvideo',
    '.mov': 'video/quicktime',
    '.wmv': 'video/x-ms-wmv',
    '.flv': 'video/x-flv',
    '.ts': 'video/mp2t',
    '.m2ts': 'video/mp2t',
    '.3gp': 'video/3gpp',
    '.mp3': 'audio/mpeg',
    '.m4a': 'audio/mp4',
    '.aac': 'audio/aac',
    '.flac': 'audio/flac',
    '.wav': 'audio/wav',
    '.ogg': 'audio/ogg',
    '.srt': 'text/plain',
    '.sub': 'text/plain',
    '.vtt': 'text/vtt'
};

// نوع افتراضي عام للامتدادات غير المعروفة - أفضل من فرض video/mp4 خطأً على ملف ليس mp4
function getContentType(filePath) {
    const ext = path.extname(filePath).toLowerCase();
    return MIME_TYPES[ext] || 'application/octet-stream';
}

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
// مرجع صريح لمخزن SlidingWindowStore الفعلي لكل تورنت - نلتقطه بأنفسنا عند إنشائه
// (بدل تخمين اسم خاصية داخلية في torrent-stream مثل engine.store أو engine.storage،
// وهو ما كان يفشل بصمت ويمنع أي عملية حذف من العمل إطلاقاً - انظر startEngine أدناه)
let engineStores = {};
let engineStatus = {}; // hash -> { state: 'loading'|'ready'|'failed', error?: string, lastAccess?: number }
let pendingQueue = []; // [{ infoHash, magnet, queuedAt }] - طلبات تنتظر دورها لأن MAX_CONCURRENT_ENGINES ممتلئ
let hardFlushCooldownUntil = 0; // بعد تنظيف طارئ، نمنع الطابور من إعادة الملء الفوري ليهدأ الاستخدام الفعلي للذاكرة (GC)

const METADATA_TIMEOUT_MS = 30000; // إذا لم تصل بيانات التورنت خلال 30 ثانية، اعتبره فاشلاً بدل تعليقه للأبد

// عدد التورنتات النشطة المسموح بها في نفس الوقت - كل واحد يخزن بياناته في الـ RAM
// (التخزين المستخدم هو memory-chunk-store، فكل ميجا تُحمَّل تبقى في الذاكرة حتى تدمير المحرك)
// على Render Free Tier (512MB RAM إجمالي)، وميزانية الكاش المحددة بـ 50MB فقط، تورنت واحد نشط
// في نفس الوقت هو الخيار الآمن الوحيد - تورنتان متزامنان قد يتجاوزان الميزانية بسهولة.
const MAX_CONCURRENT_ENGINES = parseInt(process.env.MAX_CONCURRENT_ENGINES || '1', 10);

// سقف الذاكرة "اللين" (RSS بالميجابايت) - مفيد الآن بشكل أساسي في حالة وجود أكثر من تورنت
// نشط (MAX_CONCURRENT_ENGINES > 1)، لحذف الأقل استخداماً منها وترك مكان لغيره.
// ملاحظة من المراقبة الفعلية بعد إدخال SlidingWindowStore: الاستهلاك يستقر طبيعياً حول
// 200-230MB لتورنت واحد نشط (أساس Node/Express/torrent-stream ~140-170MB + ميزانية
// نافذة التخزين ~40-60MB) - رفعنا الحد إلى 260MB بدل 200MB كي لا يُعتبر هذا الاستقرار
// الطبيعي "تجاوزاً" يستدعي أي تحذير أو إجراء، مع إبقاء هامش مريح تحت HARD_MEMORY_MB (350MB).
const MAX_MEMORY_MB = parseInt(process.env.MAX_MEMORY_MB || '260', 10);

// مهلة اعتبار البث "متوقفاً" - رُفعت من 30 إلى 90 ثانية افتراضياً. السبب: عند نت غير مستقر لدى العميل،
// Node.js يطبّق backpressure فيتوقف حدث 'data' عن الإطلاق بسبب بطء العميل نفسه وليس توقف التورنت -
// مهلة قصيرة جداً كانت تقتل البث ظلماً في هذه الحالة بالضبط.
const STREAM_STALL_TIMEOUT_MS = parseInt(process.env.STREAM_STALL_TIMEOUT_MS || '90000', 10);

// بعد كم من عدم النشاط (لا طلبات بث) يُعتبر التورنت مهجوراً ويُحذف - وليس وقتاً ثابتاً منذ الإنشاء
const IDLE_ENGINE_TIMEOUT_MS = parseInt(process.env.IDLE_ENGINE_TIMEOUT_MS || (2 * 60 * 60 * 1000), 10);

// سقف طارئ "صارم" - إذا وصلنا هنا فالخطر حقيقي (Render يقتل العملية عند 512MB).
// عند تجاوزه، نحذف كل التورنتات النشطة فوراً بدل الاكتفاء بحذف واحد فقط، لتفادي إعادة تشغيل قسري للسيرفر بالكامل.
const HARD_MEMORY_MB = parseInt(process.env.HARD_MEMORY_MB || '350', 10);

// *** الإصلاح الفعلي لمشكلة الذاكرة غير المحدودة ***
// بدل تحميل الملف كاملاً ثم محاولة حذف القطع القديمة من الذاكرة (وهو ما فشل: القطع
// البعيدة أمام رأس التشغيل كانت لا تزال تُحمَّل من الشبكة وتُحتفَظ بها بالكامل)، نتحكم
// الآن في *أي نطاق بايت من الملف يُسمح لمحرك التورنت بتحميله أصلاً* عبر engine.select/
// engine.deselect، ونُحرّك هذا النطاق تدريجياً مع تقدّم رأس التشغيل الفعلي (انظر
// attachSelectionWindow أدناه). القطع خارج هذا النطاق لا تُطلب من الشبكة إطلاقاً.
const DOWNLOAD_WINDOW_AHEAD_MB = parseFloat(process.env.CHUNK_WINDOW_AHEAD_MB || '40');
const DOWNLOAD_WINDOW_BEHIND_MB = parseFloat(process.env.CHUNK_WINDOW_BEHIND_MB || '20');
const DOWNLOAD_WINDOW_AHEAD_BYTES = DOWNLOAD_WINDOW_AHEAD_MB * 1024 * 1024;
const DOWNLOAD_WINDOW_BEHIND_BYTES = DOWNLOAD_WINDOW_BEHIND_MB * 1024 * 1024;

// ملاحظة أمانة تقنية: engine.select/deselect هنا تُستدعى بمحاذير try/catch لأن التوقيع
// الدقيق (بايت مقابل رقم قطعة) غير موثَّق رسمياً بشكل يمكن التحقق منه هنا بدون شبكة -
// إن فشل الاستدعاء لأي سبب، يُسجَّل تحذير بدل انهيار الخادم، ويستمر التحميل بلا تحديد نطاق
// (سلوك أقدم لكن آمن) - راقب اللوغ بحثاً عن "[SelectWindow] فشل" للتأكد أنها تعمل فعلاً.
function selectWindow(engine, from, to) {
    try {
        engine.select(from, to, true);
    } catch (e) {
        console.warn(`[SelectWindow] فشل استدعاء engine.select(${from}, ${to}):`, e && e.message);
    }
}
function deselectWindow(engine, from, to) {
    try {
        engine.deselect(from, to, true);
    } catch (e) {
        console.warn(`[SelectWindow] فشل استدعاء engine.deselect(${from}, ${to}):`, e && e.message);
    }
}

// ينشئ "نافذة تحميل" متحركة بالبايت لطلب بث واحد. moveTo(consumedAbsoluteByte) تُحرّك
// النطاق المسموح تحميله ليتبع ما استهلكه العميل فعلياً؛ cleanup() تُلغي التحديد عند
// انتهاء/قطع الاتصال حتى لا يبقى نطاق قديم محجوزاً للتحميل بلا داعٍ.
// تنبيه: إن فتح العميل (1DM+) عدة اتصالات متوازية لنطاقات مختلفة من نفس الملف في آنٍ
// واحد (شائع في مديري التحميل لتسريع النقل)، فكل اتصال يُدير نافذته الخاصة بشكل مستقل -
// قد يتداخل هذا مع بعضه (اتصال يُلغي تحديد نطاق لا يزال اتصال آخر يحتاجه). راقب هذا
// الاحتمال إن استمرت مشاكل الأداء رغم ثبات الذاكرة.
function attachSelectionWindow(engine, fileOffset, rangeStart, rangeEnd) {
    const absoluteRangeStart = fileOffset + rangeStart;
    const absoluteRangeEnd = fileOffset + rangeEnd;
    let currentWindow = null;

    function moveTo(consumedAbsoluteByte) {
        const newTo = Math.min(consumedAbsoluteByte + DOWNLOAD_WINDOW_AHEAD_BYTES, absoluteRangeEnd);
        const newFrom = Math.max(absoluteRangeStart, consumedAbsoluteByte - DOWNLOAD_WINDOW_BEHIND_BYTES);

        if (currentWindow) {
            const aheadAdvance = newTo - currentWindow.to;
            const behindAdvance = newFrom - currentWindow.from;
            // حدّث فقط إن تقدّمت أي من الحافتين بمقدار معتبر (نصف ميزانيتها) - وإلا تجاهل
            // (بدون هذا الشرط، كان يُستدعى select/deselect عند كل دفعة بيانات تقريباً)
            if (aheadAdvance < (DOWNLOAD_WINDOW_AHEAD_BYTES / 2) && behindAdvance < (DOWNLOAD_WINDOW_BEHIND_BYTES / 2)) return;
            deselectWindow(engine, currentWindow.from, currentWindow.to);
        }
        selectWindow(engine, newFrom, newTo);
        currentWindow = { from: newFrom, to: newTo };
    }

    function cleanup() {
        if (currentWindow) {
            deselectWindow(engine, currentWindow.from, currentWindow.to);
            currentWindow = null;
        }
    }

    moveTo(absoluteRangeStart); // نافذة أولية عند بدء الطلب
    return { moveTo, cleanup };
}

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

function sweepIdleEngines() {
    const now = Date.now();
    Object.keys(activeEngines).forEach(hash => {
        const status = engineStatus[hash];
        // لا تحذف تورنتاً لا يزال في مرحلة "loading" (له مهلته الخاصة عبر metadataTimer)
        if (!status || status.state !== 'ready') return;
        const idleFor = now - (status.lastAccess || 0);
        if (idleFor > IDLE_ENGINE_TIMEOUT_MS) {
            console.log(`[Idle Cleanup] حذف ${hash} بعد ${Math.round(idleFor / 60000)} دقيقة من عدم النشاط`);
            destroyEngine(hash);
        }
    });
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
        const activeCount = Object.keys(activeEngines).length;
        // "حذف الأقل استخداماً" مفيد فقط عندما يوجد أكثر من تورنت نشط لنختار بينها.
        // إن كان هناك تورنت واحد فقط، فهو حكماً "الأقل استخداماً" - وحذفه هنا لا يحرر
        // شيئاً مفيداً، بل يقتل التحميل الوحيد الجاري نفسه في منتصفه بلا داعٍ.
        // نافذة التخزين المتحركة (SlidingWindowStore) صُمِّمت لتستقر الذاكرة قريباً من
        // (الأساس + ميزانية النافذة)، وقد يكون هذا أعلى قليلاً من MAX_MEMORY_MB القديم -
        // هذا وضع طبيعي متوقع، وليس تسرّب ذاكرة يستوجب القتل. الحد الطارئ HARD_MEMORY_MB
        // أعلاه يبقى خط الدفاع الحقيقي الأخير إن أصبح الخطر فعلياً حقيقياً.
        if (activeCount > 1) {
            console.warn(`[Memory Monitor] تجاوزنا الحد الآمن (${MAX_MEMORY_MB} MB) - محاولة تحرير ذاكرة من تورنتات أخرى أقل استخداماً`);
            evictLeastRecentlyUsed();
        } else if (activeCount === 1) {
            console.warn(`[Memory Monitor] تجاوزنا الحد الآمن (${MAX_MEMORY_MB} MB) لكن تورنت واحد فقط نشط - لن نحذفه؛ الحد الطارئ (${HARD_MEMORY_MB}MB) وحده من سيتدخل عند الخطر الحقيقي`);
        }
        // إن لم يبقَ تورنت نشط، فالذاكرة المرتفعة هي فقط heap محجوز من Node/V8 لم يُعَد بعد لنظام التشغيل -
        // ليس هناك ما نحذفه، والتحذير المتكرر بلا فائدة حقيقية (RSS لا يعود ينخفض بمجرد الحذف، هذا سلوك طبيعي في Node)
    } else {
        processQueue(); // تأكيد دوري: استئناف الطابور تلقائياً بعد انتهاء أي فترة تهدئة
    }
    sweepIdleEngines(); // احذف فقط ما كان خاملاً فعلاً منذ فترة طويلة - وليس وقتاً ثابتاً منذ الإنشاء
}, 10000); // كل 10 ثواني بدل 60 - على بيئة بهذا الضيق (512MB)، الذاكرة قد ترتفع بسرعة أثناء البث

function destroyEngine(infoHash) {
    const engine = activeEngines[infoHash];
    if (!engine) return;
    try { engine.destroy(); } catch (e) { }
    delete activeEngines[infoHash];
    delete engineStatus[infoHash];
    delete engineStores[infoHash];
    console.log(`Cleared Engine from RAM for Hash: ${infoHash}`);
    processQueue(); // حرّرنا مكاناً - شغّل التالي في الطابور إن وُجد
}

function failEngine(infoHash, reason) {
    const engine = activeEngines[infoHash];
    if (engine) {
        try { engine.destroy(); } catch (e) { }
    }
    delete activeEngines[infoHash];
    delete engineStores[infoHash];
    engineStatus[infoHash] = { state: 'failed', error: reason };
    console.error(`[FAILED] لم يتم العثور على بيانات/سيدرز لـ ${infoHash}: ${reason}`);
    processQueue(); // حرّرنا مكاناً هنا أيضاً
}

// يبدأ محرك تورنت فعلياً لهذا الـ hash؛ يُستدعى إما مباشرة (يوجد مكان شاغر) أو من الطابور عند تحرر مكان
function startEngine(infoHash, magnet) {
    let engine;
    try {
        // نلفّ دالة المصنع لنلتقط مرجع المخزن الفعلي بأنفسنا وقت إنشائه - هذا أوثق من
        // محاولة تخمين اسم الخاصية التي يضع بها torrent-stream المرجع داخل كائن engine لاحقاً
        const capturingStorageFactory = (chunkLength, opts) => {
            const storeInstance = slidingWindowStore(chunkLength, opts);
            engineStores[infoHash] = storeInstance;
            return storeInstance;
        };
        // التخزين المخصص: نافذة متحركة من القطع حول موضع التشغيل الحالي بدل الاحتفاظ
        // بالملف كاملاً في الذاكرة (انظر sliding-window-store.js لتفاصيل الآلية)
        engine = torrentStream(magnet, { storage: capturingStorageFactory });
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

    // ملاحظة: لا نضع هنا مؤقت حذف ثابت منذ الإنشاء - المسح الدوري لعدم النشاط (انظر IDLE_ENGINE_TIMEOUT_MS
    // أدناه في المراقب الدوري) هو من يقرر الحذف بناءً على آخر استخدام فعلي، وليس وقتاً ثابتاً قد يقطع تحميلاً نشطاً
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
    let ownerEngine = null;
    Object.entries(activeEngines).forEach(([hash, engine]) => {
        if (engine.files) {
            let f = engine.files.find(file => file.path === filePath);
            if (f) { targetFile = f; ownerHash = hash; ownerEngine = engine; }
        }
    });

    if (!targetFile) return res.status(404).send('File not found or metadata loading...');

    // حدّث وقت آخر استخدام حتى لا يُحذف هذا التورنت بواسطة نظام تحرير الذاكرة (LRU) أثناء المشاهدة
    if (ownerHash && engineStatus[ownerHash]) {
        engineStatus[ownerHash].lastAccess = Date.now();
    }

    // لم نعد نستدعي targetFile.select() (كانت تطلب الملف كاملاً من الشبكة فوراً بلا حدود -
    // وهذا كان السبب الجذري لتجاوز الذاكرة رغم وجود "نافذة" ظاهرية في التخزين).
    // بدلها، كل طلب بث ينشئ نافذة تحميل متحركة خاصة به عبر attachSelectionWindow أدناه.

    // مرجع التخزين المخصص (النافذة المتحركة) الخاص بهذا المحرك - يُلتقط صراحةً وقت الإنشاء
    // في startEngine بدل تخمين خاصية داخلية في كائن engine
    const store = ownerHash && engineStores[ownerHash];
    if (!store) {
        console.warn(`[SlidingWindowStore] تحذير: لم يُعثر على مخزن مرتبط بـ ${ownerHash} - لن يعمل حذف القطع القديمة لهذا التورنت`);
    }
    // موضع هذا الملف داخل التورنت الكامل - القطع مرقّمة على مستوى التورنت وليس الملف
    const fileOffset = targetFile.offset || 0;

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
        res.setHeader('Content-Type', getContentType(filePath));
        const window = attachSelectionWindow(ownerEngine, fileOffset, 0, targetFile.length - 1);
        const stream = targetFile.createReadStream();
        let sentBytes = 0;
        stream.on('data', (chunk) => {
            sentBytes += chunk.length;
            const consumedAbsolute = fileOffset + sentBytes;
            if (store && typeof store.setPlayheadByte === 'function') {
                store.setPlayheadByte(consumedAbsolute);
            }
            window.moveTo(consumedAbsolute);
        });
        stream.on('end', () => window.cleanup());
        // هذا المسار (بلا range) لا يملك مهلة توقف ذاتية، فـ 'close' هنا تعني دائماً
        // انقطاعاً حقيقياً من العميل - آمن إلغاء التحديد عندها
        stream.on('close', () => window.cleanup());
        stream.on('error', (err) => { window.cleanup(); failSafely(err); });
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
        'Content-Type': getContentType(filePath)
    });

    const window = attachSelectionWindow(ownerEngine, fileOffset, start, end);
    const stream = targetFile.createReadStream({ start, end });
    let sentRangeBytes = 0;
    let destroyedByOurTimeout = false; // true فقط عندما نحن من قطعنا الاتصال بسبب التوقف المؤقت

    let streamTimeout = setTimeout(() => {
        console.error(`[Timeout] تم قطع الاتصال المعلق لـ "${filePath}".`);
        destroyedByOurTimeout = true;
        stream.destroy();
        if (!res.headersSent) res.status(504).send('Torrent stream stalled (No Seeders)');
    }, STREAM_STALL_TIMEOUT_MS);

    stream.on('data', (chunk) => {
        clearTimeout(streamTimeout);
        streamTimeout = setTimeout(() => {
            console.error(`[Timeout] تم قطع الاتصال المعلق لـ "${filePath}".`);
            destroyedByOurTimeout = true;
            stream.destroy();
        }, STREAM_STALL_TIMEOUT_MS);
        sentRangeBytes += chunk.length;
        const consumedAbsolute = fileOffset + start + sentRangeBytes;
        if (store && typeof store.setPlayheadByte === 'function') {
            store.setPlayheadByte(consumedAbsolute);
        }
        window.moveTo(consumedAbsolute);
    });

    stream.on('end', () => { clearTimeout(streamTimeout); window.cleanup(); });
    // نُلغي التحديد عند 'close' فقط إن كان السبب انقطاعاً حقيقياً من العميل (مثلاً ألغى
    // 1DM+ التحميل فعلياً) - وليس توقفنا المؤقت الذاتي، الذي غالباً يتبعه إعادة محاولة
    // فورية من العميل على نفس النطاق تقريباً؛ إلغاء التحديد حينها كان يصفّر تقدّمها.
    stream.on('close', () => {
        if (!destroyedByOurTimeout) window.cleanup();
    });
    stream.on('error', (err) => {
        clearTimeout(streamTimeout);
        window.cleanup();
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
