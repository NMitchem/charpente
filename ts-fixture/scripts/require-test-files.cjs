// `node --test` exits 0 having run zero tests when its glob matches nothing —
// verified: an empty `dist/**/*.test.js` match prints "tests 0" and exits 0,
// with or without recursion. That is precisely the failure chunk-b0-brief.md's
// exit criterion 1 rules out ("a runner that exits 0 having run nothing does
// not count"), just reachable a different way (every test file renamed,
// moved out of `dist/`, or excluded by a future tsconfig change) — so this is
// the floor check underneath the glob, run before node --test ever starts.
const fs = require("node:fs");
const path = require("node:path");

const dist = path.join(__dirname, "..", "dist");
const found = fs
  .readdirSync(dist, { recursive: true })
  .filter((f) => f.endsWith(".test.js"));

if (found.length === 0) {
  console.error(
    "require-test-files: no *.test.js files found anywhere under dist/ — " +
      "`node --test` would report \"tests 0\" and exit 0, which this repo " +
      "treats as a broken run, not a passing one (chunk-b0-brief.md exit " +
      "criterion 1). Refusing to proceed.",
  );
  process.exit(1);
}
