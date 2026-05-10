// Test-only Context augmentation. Imported for side effects by test files
// that target rules on `plan` / `country`.
import "../types.js";

declare module "../types.js" {
  interface Context {
    plan?: "free" | "pro" | "enterprise";
    country?: string;
  }
}

export {};
