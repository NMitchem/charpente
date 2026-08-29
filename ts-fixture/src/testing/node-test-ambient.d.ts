// Minimal ambient types for Node's built-in test runner and assert module.
// `@types/node` would normally supply these, but the execution route (chunk
// B0, ts-client-codegen) is zero new dependencies — including type-only ones
// — so this hand-declares exactly the surface `*.test.ts` files use. Widen it
// only when a test needs more of the real API.

declare module "node:test" {
  export function test(name: string, fn: () => void | Promise<void>): void;
}

declare module "node:assert/strict" {
  export function equal(actual: unknown, expected: unknown, message?: string): void;
  export function deepEqual(actual: unknown, expected: unknown, message?: string): void;
  export function ok(value: unknown, message?: string): void;
}
