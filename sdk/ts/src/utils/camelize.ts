/**
 * Recursively transforms all object keys from snake_case to camelCase.
 * Values (including string literals used as discriminants) are untouched.
 */
type CamelKey<S extends string> = S extends `${infer P}_${infer Q}${infer R}`
  ? `${P}${Uppercase<Q>}${CamelKey<R>}`
  : S;

export type Camelize<T> = T extends null | undefined ? T
  : T extends (infer U)[] ? Camelize<U>[]
  : T extends object
    ? { [K in keyof T as CamelKey<K & string>]: Camelize<T[K]> }
  : T;

export function camelize<T>(obj: T): Camelize<T> {
  if (obj === null || obj === undefined) return obj as Camelize<T>;
  if (Array.isArray(obj)) return obj.map(camelize) as Camelize<T>;
  if (typeof obj === "object") {
    const result: Record<string, unknown> = {};
    for (
      const [key, value] of Object.entries(
        obj as Record<string, unknown>,
      )
    ) {
      const camel = key.replace(/_([a-z])/g, (_, c: string) => c.toUpperCase());
      result[camel] = camelize(value);
    }
    return result as Camelize<T>;
  }
  return obj as Camelize<T>;
}

/**
 * Shallow snake_case transformation — only top-level keys are renamed.
 * Used for request bodies where nested values (e.g. WebAuthn credentials)
 * must not be touched.
 */
export function snakenize(
  obj: Record<string, unknown>,
): Record<string, unknown> {
  const result: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(obj)) {
    const snake = key.replace(/[A-Z]/g, (c) => `_${c.toLowerCase()}`);
    result[snake] = value;
  }
  return result;
}
