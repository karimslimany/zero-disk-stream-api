const express = require('express');
const cors = require('cors');
const torrentStream = require('torrent-stream');
const memoryStore = require('memory-chunk-store');

const app = express();
app.use(cors());
app.use(express.json());

// مجلد لحفظ محركات البحث النشطة في الذاكرة
let activeEngines = {};

// شبكة أمان أخيرة: لو حصل أي خطأ غير متوقع لم تتم معالجته بأي مكان آخر في
// الكود، نسجّله فقط في الـ log ولا نسمح له بإيقاف السيرفر بالكامل. هذا لا يحل
// المشكلة الجذرية، لكنه يضمن استمرار السيرفر بالعمل لباقي المستخدمين
process.on('uncaughtException', (err) => {
    console.error('[uncaughtException] السيرفر استمر بالعمل رغم هذا الخطأ:', err.message);
});
process.on('unhandledRejection', (err) => {
    console.error('[unhandledRejection] السيرفر استمر بالعمل رغم هذا الخطأ:', err && err.message);
});

// دالة مساعدة لحذف محرك تورنت بأمان مع تنظيف كل مستمعيه
function destroyEngine(infoHash) {
    const engine = activeEngines[infoHash];
    if (!engine) return;
    try { engine.destroy(); } catch (e) { /* تجاهل أي خطأ أثناء الإغلاق نفسه */ }
    delete activeEngines[infoHash];
    console.log(`Cleared RAM for Hash: ${infoHash}`);
}

// 1. استقبال الماغنيت بنفس صيغة الـ API القديمة تماماً ليتوافق مع موقعك
app.post('/api/v1/torrents', (req, res) => {
    const { magnet } = req.body || {};
    if (!magnet) return res.status(400).json({ error: "Missing magnet Link" });

    let hashMatch = magnet.match(/btih:([a-zA-Z0-9]{32,40})/i);
    if (!hashMatch) return res.status(400).json({ error: "Invalid magnet hash" });
    let infoHash = hashMatch[1].toLowerCase();

    if (!activeEngines[infoHash]) {
        let engine;
        try {
            // السحر البرمجي هنا: تشغيل التورنت في الذاكرة العشوائية RAM بنسبة 100%
            engine = torrentStream(magnet, { storage: memoryStore });
        } catch (err) {
            console.error(`فشل إنشاء محرك التورنت لـ ${infoHash}:`, err.message);
            return res.status(500).json({ error: "Failed to start torrent engine" });
        }

        // معالجة أي خطأ يصدر من المحرك نفسه (فشل الاتصال بالـ trackers، أو
        // مشاكل الشبكة...) بدل تركه بلا معالج يوقف السيرفر بالكامل
        engine.on('error', (err) => {
            console.error(`خطأ في محرك التورنت لـ ${infoHash}:`, err && err.message);
            destroyEngine(infoHash);
        });

        engine.on('ready', () => {
            activeEngines[infoHash] = engine;
            console.log(`Torrent Ready in RAM: ${engine.torrent.name}`);
        });

        // حماية السيرفر من الانهيار: تدمير المحرك تلقائياً بعد ساعتين لتفريغ الـ RAM
        setTimeout(() => destroyEngine(infoHash), 2 * 60 * 60 * 1000);
    }

    res.json({ status: "added", hash: infoHash });
});

// 2. تزويد واجهة موقعك بنفس البيانات والهيكلية لمطابقة الملفات دون تعديل فرونت-إند
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

    // دالة مساعدة تتأكد أننا لا نحاول إرسال رد ثانٍ بعد فشل حدث بالستريم
    // (لأنه لو كانت الترويسات أُرسلت للمتصفح فعلاً، لا يمكن تغيير حالة الرد)
    const failSafely = (err) => {
        console.error(`خطأ أثناء بث الملف "${filePath}":`, err && err.message);
        if (!res.headersSent) {
            res.status(502).send('Streaming failed, please try again');
        } else {
            res.destroy();
        }
    };

    const range = req.headers.range;

    // إذا كان الطلب عادياً بدون تقسيم (طلب بث مباشر للمشاهدة مثلاً)
    if (!range) {
        res.setHeader('Content-Length', targetFile.length);
        res.setHeader('Accept-Ranges', 'bytes');
        const stream = targetFile.createReadStream();
        // *** هذا هو الإصلاح الجوهري: معالجة خطأ الستريم بدل تركه يُسقط السيرفر بالكامل ***
        stream.on('error', failSafely);
        return stream.pipe(res);
    }

    // هندسة الحزم (Byte-Range Math) لضمان تفعيل الـ Resume والـ Multi-connections في 1DM+
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
        'Content-Type': 'video/mp4' // التمرير كتدفق فيديو خام ليقرأه التطبيق فوراً
    });

    // سحب القطع المطلوبة فقط تدفقياً من شبكة التورنت وتمريرها فوراً للهاتف
    const stream = targetFile.createReadStream({ start, end });
    // *** نفس الإصلاح هنا أيضاً — هذا هو المسار الذي يستخدمه 1DM+ فعلياً (Range requests) ***
    stream.on('error', failSafely);
    stream.pipe(res);
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => console.log(`Zero-Disk Streaming API running on port ${PORT}`));
