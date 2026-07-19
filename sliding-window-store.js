'use strict';

/**
 * sliding-window-store.js
 * -------------------------------------------------------------------------
 * بديل عن memory-chunk-store مصمم خصيصاً لبث الفيديو بذاكرة محدودة.
 *
 * الفرق الجوهري عن memory-chunk-store:
 *   - memory-chunk-store: يقبل أي قطعة (chunk) تصل من الشبكة ويحتفظ بها للأبد
 *     في مصفوفة، فحجم الذاكرة يكبر خطياً مع حجم الملف كاملاً.
 *   - SlidingWindowStore: يحتفظ فقط بنافذة محدودة من القطع حول "رأس التشغيل"
 *     الفعلي (byte offset الذي يُبَث للعميل الآن)، ويحذف القطع الأقدم فوراً،
 *     ويؤخّر (backpressure) قبول القطع الأحدث مما يسمح به إن كانت بعيدة جداً
 *     أمام رأس التشغيل - بدل تركها تتكدس في الذاكرة أثناء تحميلها من الشبكة.
 *
 * هذا يجعل الذاكرة المستهلكة تقريباً ثابتة ومحدَّدة صراحةً بالميجابايت عبر
 * CHUNK_WINDOW_AHEAD_MB و CHUNK_WINDOW_BEHIND_MB (وليس بعدد قطع ثابت - لأن حجم
 * القطعة نفسه يتغيّر حسب حجم التورنت، فعدد قطع ثابت لا يعطي سقف ذاكرة موثوق)،
 * بغض النظر عن حجم أو مدة الملف بالكامل.
 *
 * ملاحظة أمانة تقنية مهمة:
 * تأخير استدعاء الـ callback داخل put() هو ما يخلق الضغط الخلفي (backpressure)
 * الذي يمنع محرك التورنت من طلب المزيد من القطع من الشبكة بلا حدود. هذا السلوك
 * يعتمد على أن torrent-stream/bittorrent-swarm ينتظران فعلاً انتهاء put() قبل
 * تحرير "خانة الطلب" (request slot) الخاصة بتلك القطعة - وهذا صحيح في التطبيقات
 * الشائعة لواجهة chunk-store، لكنه غير موثّق رسمياً كضمان صارم 100%. يُنصح
 * باختبار هذا فعلياً تحت حمل حقيقي (تنزيل + بث متزامن) قبل الاعتماد عليه إنتاجياً،
 * وأن يبقى HARD_MEMORY_MB (الموجود أصلاً في index.js) كشبكة أمان أخيرة.
 */

const EventEmitter = require('events');

function pieceIndexForByte(byteOffset, chunkLength) {
    return Math.floor(byteOffset / chunkLength);
}

class SlidingWindowStore extends EventEmitter {
    constructor(chunkLength, opts) {
        super();
        opts = opts || {};

        this.chunkLength = chunkLength;
        this.length = opts.length || 0;
        this.closed = false;

        // خريطة index -> Buffer، بدل مصفوفة كاملة الحجم كما في memory-chunk-store
        this.chunks = new Map();

        // "رأس التشغيل" الحالي بوحدة رقم القطعة - يُحدَّث من الخارج عبر setPlayheadByte
        // كلما بُثّت بايتات فعلية للعميل (انظر ربطها بـ /data/* في index.js)
        this.playheadIndex = 0;

        // *** مهم: النافذة تُحدَّد بالميجابايت وليس بعدد القطع ***
        // حجم القطعة (piece length) في BitTorrent يتغيّر تلقائياً حسب حجم التورنت الكلي:
        // قد يكون 256KB لتورنت صغير، وقد يصل إلى 4-16MB لتورنت ضخم (عدة جيجابايت).
        // فلو ثبّتنا "عدد القطع" بدل الحجم، فإن 20 قطعة قد تعني 5MB في حالة وتعني
        // 300+MB في حالة أخرى - أي لا ضمان فعلي لسقف الذاكرة. لذلك نحسب الميزانية
        // بالبايت أولاً، ثم نشتق عدد القطع المقابل من chunkLength الفعلي لهذا التورنت.
        const aheadMB = opts.aheadWindowMB || parseFloat(process.env.CHUNK_WINDOW_AHEAD_MB || '40');
        const behindMB = opts.behindWindowMB || parseFloat(process.env.CHUNK_WINDOW_BEHIND_MB || '20');
        const aheadBytes = aheadMB * 1024 * 1024;
        const behindBytes = behindMB * 1024 * 1024;

        // على الأقل قطعة واحدة أمام رأس التشغيل - وإلا يتوقف البث تماماً (لا نستطيع تخزين صفر قطع)
        this.aheadWindow = Math.max(1, Math.floor(aheadBytes / chunkLength));
        this.behindWindow = Math.max(0, Math.floor(behindBytes / chunkLength));

        // تحذير صريح: إن كانت القطعة الواحدة أكبر من ميزانية الأمام المحددة، فسقف الذاكرة
        // الفعلي لهذا التورنت سيكون أعلى مما طلبته (لأننا لا نستطيع تخزين أقل من قطعة واحدة).
        // هذا وارد فقط مع تورنتات ضخمة جداً بقطع كبيرة جداً - راقب [Memory Monitor] في هذه الحالة.
        if (chunkLength > aheadBytes) {
            console.warn(
                `[SlidingWindowStore] تحذير: حجم القطعة (${(chunkLength / 1024 / 1024).toFixed(2)}MB) ` +
                `أكبر من ميزانية النافذة الأمامية المطلوبة (${aheadMB}MB). ` +
                `الحد الأدنى الفعلي للذاكرة سيكون قطعة واحدة = ${(chunkLength / 1024 / 1024).toFixed(2)}MB بدل ${aheadMB}MB.`
            );
        }

        console.log(
            `[SlidingWindowStore] chunkLength=${(chunkLength / 1024).toFixed(0)}KB | ` +
            `aheadWindow=${this.aheadWindow} قطعة (~${aheadMB}MB) | ` +
            `behindWindow=${this.behindWindow} قطعة (~${behindMB}MB) | ` +
            `سقف الذاكرة التقريبي لهذا التورنت ≈ ${(((this.aheadWindow + this.behindWindow + 1) * chunkLength) / 1024 / 1024).toFixed(1)}MB`
        );

        // القطع التي وصلت من الشبكة لكنها أبعد من المسموح - callback الخاص بها مؤجل عمداً
        this._pendingPuts = [];
    }

    // يُستدعى من نقطة بث /data/* في index.js عند كل دفعة بيانات فعلية تُرسل للعميل
    setPlayheadByte(byteOffset) {
        const idx = pieceIndexForByte(byteOffset, this.chunkLength);
        if (idx > this.playheadIndex) {
            this.playheadIndex = idx;
            this._evictBehind();
            this._releasePendingPuts();
        }
    }

    _evictBehind() {
        const floor = this.playheadIndex - this.behindWindow;
        if (floor <= 0) return;
        for (const idx of this.chunks.keys()) {
            if (idx < floor) this.chunks.delete(idx);
        }
    }

    _releasePendingPuts() {
        // بعد تقدم رأس التشغيل قد تتحرر مساحة ضمن النافذة الأمامية - أفرج عن القطع المعلّقة بالترتيب
        this._pendingPuts.sort((a, b) => a.index - b.index);
        while (
            this._pendingPuts.length &&
            (this._pendingPuts[0].index - this.playheadIndex) <= this.aheadWindow
        ) {
            const p = this._pendingPuts.shift();
            this.chunks.set(p.index, p.buf);
            p.cb(null);
        }
    }

    put(index, buf, cb) {
        cb = cb || function () {};
        if (this.closed) {
            return process.nextTick(() => cb(new Error('Storage closed')));
        }

        const distanceAhead = index - this.playheadIndex;
        if (distanceAhead > this.aheadWindow) {
            // هذه القطعة أبعد مما نريد تخزينه الآن - لا تُخزَّن ولا يُستدعى cb بعد.
            // هذا التأخير هو آلية الضغط الخلفي التي تمنع تكدس الذاكرة.
            this._pendingPuts.push({ index, buf, cb });
            return;
        }

        this.chunks.set(index, buf);
        this._evictBehind();
        process.nextTick(() => cb(null));
    }

    get(index, opts, cb) {
        if (typeof opts === 'function') {
            cb = opts;
            opts = {};
        }
        opts = opts || {};
        if (this.closed) {
            return process.nextTick(() => cb(new Error('Storage closed')));
        }

        const buf = this.chunks.get(index);
        if (!buf) {
            // إما لم تصل القطعة بعد من الشبكة، أو حُذفت لأنها أقدم من نافذة التشغيل.
            // نُرجع خطأً - وهو السلوك المتوقع من واجهة chunk-store عند غياب القطعة.
            return process.nextTick(() =>
                cb(new Error('Chunk not found (evicted or not downloaded yet)'))
            );
        }

        let out = buf;
        if (opts.offset || opts.length) {
            const start = opts.offset || 0;
            const end = opts.length ? start + opts.length : buf.length;
            out = buf.slice(start, end);
        }
        process.nextTick(() => cb(null, out));
    }

    close(cb) {
        this.closed = true;
        this.chunks.clear();
        this._pendingPuts.forEach((p) => p.cb(new Error('Storage closed')));
        this._pendingPuts = [];
        process.nextTick(() => cb && cb(null));
    }

    destroy(cb) {
        this.close(cb);
    }
}

// torrent-stream يستدعي options.storage كدالة مصنع: storage(chunkLength, opts)
// تماماً كما تفعل memory-chunk-store و fs-chunk-store
module.exports = function createSlidingWindowStore(chunkLength, opts) {
    return new SlidingWindowStore(chunkLength, opts);
};
module.exports.SlidingWindowStore = SlidingWindowStore;
