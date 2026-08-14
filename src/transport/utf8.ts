// UTF-8 decoding across chunk boundaries.
//
// Both transports receive bytes in pieces chosen by the network, not by
// the protocol, so a multi-byte character can straddle two of them (a
// server banner is free-form UTF-8). Decoding each piece independently
// turns such a character into replacement characters in BOTH halves and
// corrupts the JSON line carrying it.
//
// Neither half of the job can be delegated to `TextDecoder`, because
// React Native's polyfill accepts its options and ignores them — on-device
// CI proved both: `{ stream: true }` decoded a split '€' as three
// replacement characters, and `{ fatal: true }` returned U+FFFD for bytes
// that are not UTF-8 at all instead of throwing. Silent replacement is
// the dangerous half: it leaves the surrounding JSON syntactically valid,
// so corruption inside a txid, an address or a script reaches the caller
// as data.
//
// So both are done here, in code that behaves the same on every runtime:
// the byte stream is validated by this file's own scanner, an incomplete
// trailing character is carried to the next chunk, and the platform
// decoder only ever sees bytes already known to be whole and valid.

/** Longest UTF-8 sequence, and therefore the most bytes we ever hold. */
const MAX_SEQUENCE = 4;

/** Thrown for bytes that are not valid UTF-8 at all (not merely split). */
export class Utf8DecodeError extends Error {
  override readonly name = 'Utf8DecodeError';
}

/**
 * Continuation-byte range for the SECOND byte of a sequence. The lead
 * byte narrows it: the general 0x80-0xBF would also admit overlong forms
 * (C0/C1), UTF-16 surrogates (ED A0..BF) and code points above U+10FFFF
 * (F4 90..BF) — all of which RFC 3629 rejects, and all of which a decoder
 * that "repairs" them would hand back as replacement characters.
 */
function secondByteRange(lead: number): { min: number; max: number } | null {
  if (lead >= 0xc2 && lead <= 0xdf) return { min: 0x80, max: 0xbf };
  if (lead === 0xe0) return { min: 0xa0, max: 0xbf };
  if (lead >= 0xe1 && lead <= 0xec) return { min: 0x80, max: 0xbf };
  if (lead === 0xed) return { min: 0x80, max: 0x9f }; // no surrogates
  if (lead === 0xee || lead === 0xef) return { min: 0x80, max: 0xbf };
  if (lead === 0xf0) return { min: 0x90, max: 0xbf };
  if (lead >= 0xf1 && lead <= 0xf3) return { min: 0x80, max: 0xbf };
  if (lead === 0xf4) return { min: 0x80, max: 0x8f }; // U+10FFFF is the last
  return null; // 0x80-0xC1 (continuation / overlong) and 0xF5-0xFF
}

/** Total length of the sequence a lead byte starts. */
function sequenceLength(lead: number): number {
  if (lead < 0x80) return 1;
  if (lead <= 0xdf) return 2;
  if (lead <= 0xef) return 3;
  return 4;
}

/**
 * Validate `bytes` as UTF-8 and report where an incomplete trailing
 * character begins.
 *
 * @returns the offset of the trailing incomplete sequence, or
 *   `bytes.length` when the buffer ends on a character boundary.
 * @throws {Utf8DecodeError} on anything that is not valid UTF-8.
 */
export function scanUtf8(bytes: Uint8Array): number {
  let i = 0;
  while (i < bytes.length) {
    const lead = bytes[i]!;
    if (lead < 0x80) {
      i++;
      continue;
    }
    const range = secondByteRange(lead);
    if (range === null) {
      throw new Utf8DecodeError(`invalid UTF-8 lead byte 0x${lead.toString(16)} at offset ${i}`);
    }
    const len = sequenceLength(lead);
    // Not all here yet: legal so far, so hold it for the next chunk. Its
    // continuation bytes are validated once they arrive.
    if (i + len > bytes.length) {
      for (let k = i + 1; k < bytes.length; k++) {
        const b = bytes[k]!;
        const ok = k === i + 1 ? b >= range.min && b <= range.max : b >= 0x80 && b <= 0xbf;
        if (!ok) {
          throw new Utf8DecodeError(`invalid UTF-8 continuation byte at offset ${k}`);
        }
      }
      return i;
    }
    for (let k = i + 1; k < i + len; k++) {
      const b = bytes[k]!;
      const ok = k === i + 1 ? b >= range.min && b <= range.max : b >= 0x80 && b <= 0xbf;
      if (!ok) {
        throw new Utf8DecodeError(`invalid UTF-8 continuation byte at offset ${k}`);
      }
    }
    i += len;
  }
  return bytes.length;
}

/**
 * Turn already-validated UTF-8 into a string without a platform decoder.
 *
 * `scanUtf8` has accepted these bytes, so this only has to assemble code
 * points — no error handling, no BOM rule, identical everywhere.
 */
export function decodeValidated(bytes: Uint8Array): string {
  // Batched so `fromCodePoint` never gets an argument list long enough to
  // overflow the call stack on a large frame.
  const BATCH = 4096;
  const out: string[] = [];
  let batch: number[] = [];
  for (let i = 0; i < bytes.length; ) {
    const lead = bytes[i]!;
    let cp: number;
    if (lead < 0x80) {
      cp = lead;
      i += 1;
    } else if (lead < 0xe0) {
      cp = ((lead & 0x1f) << 6) | (bytes[i + 1]! & 0x3f);
      i += 2;
    } else if (lead < 0xf0) {
      cp = ((lead & 0x0f) << 12) | ((bytes[i + 1]! & 0x3f) << 6) | (bytes[i + 2]! & 0x3f);
      i += 3;
    } else {
      cp =
        ((lead & 0x07) << 18) |
        ((bytes[i + 1]! & 0x3f) << 12) |
        ((bytes[i + 2]! & 0x3f) << 6) |
        (bytes[i + 3]! & 0x3f);
      i += 4;
    }
    batch.push(cp);
    if (batch.length >= BATCH) {
      out.push(String.fromCodePoint(...batch));
      batch = [];
    }
  }
  if (batch.length > 0) out.push(String.fromCodePoint(...batch));
  return out.join('');
}

/**
 * The platform decoder, but only if it can be trusted with the one option
 * that matters here.
 *
 * `TextDecoder` strips a leading BOM unless `ignoreBOM` is set, and this
 * class decodes chunk by chunk — so a U+FEFF that happens to start a
 * chunk would silently disappear, making the output depend on where the
 * network split the stream. React Native's polyfill accepts `ignoreBOM`
 * and ignores it (as it does `stream` and `fatal`), so the option is
 * probed rather than trusted; when it fails, decoding falls back to
 * `decodeValidated`.
 */
function platformDecoder(): TextDecoder | null {
  if (typeof TextDecoder === 'undefined') return null;
  try {
    const d = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true });
    return d.decode(new Uint8Array([0xef, 0xbb, 0xbf])).length === 1 ? d : null;
  } catch {
    return null;
  }
}

/**
 * One decoder per connect attempt. `decode` returns the text completed so
 * far; the trailing bytes of a character split across chunks are held
 * until the chunk that finishes it.
 */
export class Utf8Stream {
  private pending: Uint8Array | null = null;
  /** Probed per stream: cheap, and no hidden global state to reset. */
  private readonly decoder: TextDecoder | null = platformDecoder();

  /** True while a character's remaining bytes are still expected. */
  hasPending(): boolean {
    return this.pending !== null;
  }

  /** Drop any half-received character. */
  reset(): void {
    this.pending = null;
  }

  /**
   * @throws {Utf8DecodeError} when the bytes are not valid UTF-8.
   */
  decode(chunk: Uint8Array): string {
    let bytes = chunk;
    if (this.pending) {
      const joined = new Uint8Array(this.pending.length + chunk.length);
      joined.set(this.pending);
      joined.set(chunk, this.pending.length);
      bytes = joined;
      // Cleared before validating: a chunk that turns out to be garbage
      // must not leave the old tail behind for the next one.
      this.pending = null;
    }
    const boundary = scanUtf8(bytes);
    if (boundary < bytes.length) {
      // Bounded by construction: a sequence is at most MAX_SEQUENCE long,
      // so at most MAX_SEQUENCE - 1 bytes are ever retained.
      this.pending = bytes.slice(boundary, Math.min(bytes.length, boundary + MAX_SEQUENCE));
      bytes = bytes.subarray(0, boundary);
    }
    if (bytes.length === 0) return '';
    // `scanUtf8` already accepted these bytes, so the platform decoder is
    // only doing the assembly work — it is used for speed, and only where
    // it has been shown to behave.
    return this.decoder ? this.decoder.decode(bytes) : decodeValidated(bytes);
  }
}

/** Normalize whatever a transport handed us into bytes, if it is bytes. */
export function asBytes(data: unknown): Uint8Array | undefined {
  if (data instanceof ArrayBuffer) return new Uint8Array(data);
  if (ArrayBuffer.isView(data)) {
    const view = data as ArrayBufferView;
    return new Uint8Array(view.buffer, view.byteOffset, view.byteLength);
  }
  return undefined;
}
