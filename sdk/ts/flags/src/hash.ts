/**
 * 32-bit FNV-1a. Stable across Node, Deno, Bun, workerd. Used to compute
 * deterministic rollout buckets — `bucket(id, name) % 100 < percent`.
 *
 * Not cryptographic; intentionally cheap. Output distribution is uniform
 * enough for percentage rollouts up to N ~ 10^7 distinct ids per flag.
 *
 * Non-ASCII characters (code > 0xFF) are fed as two bytes (high byte first)
 * so that code points sharing the same low byte hash to distinct values
 * (e.g. U+00AC '¬' and U+20AC '€' produce different buckets).
 * ASCII-only inputs are unaffected.
 */
export function fnv1a32(input: string): number {
  let hash = 0x811c_9dc5;
  for (let i = 0; i < input.length; i++) {
    const code = input.charCodeAt(i);
    if (code > 0xff) {
      // Feed the high byte first so BMP characters are uniquely represented.
      hash ^= (code >>> 8) & 0xff;
      // FNV prime: 16777619 — done with shifts to avoid Math.imul portability quirks.
      hash = (hash
        + ((hash << 1) + (hash << 4) + (hash << 7) + (hash << 8)
          + (hash << 24))) >>> 0;
    }
    hash ^= code & 0xff;
    hash = (hash
      + ((hash << 1) + (hash << 4) + (hash << 7) + (hash << 8)
        + (hash << 24))) >>> 0;
  }
  return hash >>> 0;
}

/**
 * Compute the rollout bucket for `(id, flagName)`. Returns an integer in
 * `[0, 100)`. Returning `< percent` means the context is in the rollout cohort.
 */
export function bucket(id: string, flagName: string): number {
  return fnv1a32(`${id}:${flagName}`) % 100;
}
