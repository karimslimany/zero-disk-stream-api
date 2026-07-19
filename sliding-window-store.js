'use strict';

/**
 * sliding-window-store.js
 * -------------------------------------------------------------------------
 * بديل عن memory-chunk-store، لكنه الآن يقوم بدور واحد فقط: تخزين القطع التي
 * سُمح فعلياً بتحميلها (عبر التحكم في نطاق التحديد engine.select/deselect في
 * index.js)، وحذف القطع الأقدم من "رأس التشغيل" فوراً بعد بثّها للعميل.
 *
 * *** تصحيح مهم عن النسخة السابقة ***
 * كانت هناك آلية "ضغط خلفي" (backpressure) تؤجّل استدعاء cb للقطع البعيدة جداً
 * أمام رأس التشغيل، عبر وضعها في طابور `_pendingPuts` بدل تخزينها في `chunks`.
 * لكن هذا كان عديم الفائدة فعلياً: الـ buffer نفسه (الذي وصل من الشبكة بالفعل)
 * كان لا يزال محتفَظاً به بالكامل في الذاكرة داخل ذلك الطابور - لم نكن نوفّر أي
 * ذاكرة، فقط ننقلها من مكان لآخر. هذا ما فسّر استمرار الارتفاع غير المحدود رغم
 * وجود "نافذة" ظاهرياً. تم حذف هذه الآلية بالكامل.
 *
 * الحل الصحيح: منع تحميل القطع البعيدة من الشبكة أصلاً عبر engine.select/deselect
 * (نطاق تحديد متحرك بالبايت يتبع رأس التشغيل)، بدل تحميلها ثم محاولة "التخلص" منها
 * بعد فوات الأوان. هذا الملف الآن مسؤول فقط عن الجزء الثاني (حذف القديم بعد بثّه)،
 * والتحكم في الجزء الأول (منع تحميل الجديد قبل أوانه) منقول إلى index.js.
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

        // كم قطعة نُبقيها خلف رأس التشغيل (تم بثّها فعلاً) - تُفيد فقط في حال الرجوع للخلف (seek)
        const behindMB = opts.behindWindowMB || parseFloat(process.env.CHUNK_WINDOW_BEHIND_MB || '20');
        const behindBytes = behindMB * 1024 * 1024;
        this.behindWindow = Math.max(0, Math.floor(behindBytes / chunkLength));

        console.log(
            `[SlidingWindowStore] chunkLength=${(chunkLength / 1024).toFixed(0)}KB | ` +
            `behindWindow=${this.behindWindow} قطعة (~${behindMB}MB) | ` +
            `(ضبط نطاق التحميل الأمامي الآن عبر engine.select في index.js وليس هنا)`
        );
    }

    // يُستدعى من نقطة بث /data/* في index.js عند كل دفعة بيانات فعلية تُرسل للعميل
    setPlayheadByte(byteOffset) {
        const idx = pieceIndexForByte(byteOffset, this.chunkLength);
        if (idx > this.playheadIndex) {
            this.playheadIndex = idx;
            this._evictBehind();
        }
    }

    _evictBehind() {
        const floor = this.playheadIndex - this.behindWindow;
        if (floor <= 0) return;
        for (const idx of this.chunks.keys()) {
            if (idx < floor) this.chunks.delete(idx);
        }
    }

    // للمراقبة/التشخيص فقط - كم قطعة محتفَظ بها فعلياً الآن في الذاكرة
    debugStats() {
        return {
            storedChunks: this.chunks.size,
            approxMB: ((this.chunks.size * this.chunkLength) / 1024 / 1024).toFixed(1),
            playheadIndex: this.playheadIndex
        };
    }

    put(index, buf, cb) {
        cb = cb || function () {};
        if (this.closed) {
            return process.nextTick(() => cb(new Error('Storage closed')));
        }
        // لم يعد هناك تأجيل/ضغط خلفي هنا - أي قطعة تصل فعلاً من الشبكة تُخزَّن فوراً.
        // منع وصول القطع البعيدة أصلاً هو مسؤولية engine.select/deselect في index.js.
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
