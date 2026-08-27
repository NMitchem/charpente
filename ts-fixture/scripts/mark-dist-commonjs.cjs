// `dist/` sits inside this package, whose package.json says `"type": "module"` —
// so without this, every file `tsconfig.test.json` emits there would be loaded as
// an ES module and immediately crash on its own `require()`/`exports` calls
// ("ReferenceError: exports is not defined in ES module scope"). Node's documented
// fix for exactly this is a package.json nested closer to the files that
// overrides `type` for everything under it, which is what this writes. See
// `tsconfig.test.json`'s comment for why the emitted format is CommonJS at all.
const fs = require("node:fs");
const path = require("node:path");

fs.writeFileSync(
  path.join(__dirname, "..", "dist", "package.json"),
  '{"type":"commonjs"}\n',
);
