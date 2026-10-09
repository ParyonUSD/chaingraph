// cspell:ignore bytewise clickhouse nullness Milli unhex prefilter denormalised selftest orged unnest
/**
 * The `sum` chunk digest: per row md5(s), split into two big-endian 64-bit
 * halves, each summed over the chunk mod 2^64, printed as 32 hex characters.
 * Order-independent and additive (the digest of a union of disjoint chunks is
 * the sum of their digests), so engines can aggregate in any order and
 * windows roll up into cumulative digests.
 */
import { createHash } from 'node:crypto';

const modulus = 1n << 64n;

/** Reduce engine sums to the 32-hex-char chunk digest (two 64-bit halves mod 2^64). */
export const digestFromSums = (a, b) =>
  [a, b]
    .map((value) =>
      (((BigInt(value) % modulus) + modulus) % modulus)
        .toString(16)
        .padStart(16, '0')
    )
    .join('');

/** Reference implementation of the `sum` digest in JS (used by the self-test). */
export const referenceDigest = (strings) => {
  let a = 0n;
  let b = 0n;
  for (const value of strings) {
    const md5 = createHash('md5').update(value).digest();
    a = (a + md5.readBigUInt64BE(0)) % modulus;
    b = (b + md5.readBigUInt64BE(8)) % modulus;
  }
  return digestFromSums(a, b);
};

export const addDigests = (left, right) =>
  digestFromSums(
    BigInt(`0x${left.slice(0, 16)}`) + BigInt(`0x${right.slice(0, 16)}`),
    BigInt(`0x${left.slice(16)}`) + BigInt(`0x${right.slice(16)}`)
  );
