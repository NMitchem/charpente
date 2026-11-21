# charpente launch demo: code whose test passes, the simulator's double charge, the fix, green.
# Render: vhs docs/demo/double-charge.tape   (writes double-charge.gif and double-charge.mp4 to the
# current directory; keep renders out of git)
# Every line on screen is real output from charpente 0.6.0 as installed from crates.io
# (`cargo install charpente-cli`), run on a clone of this repository; nothing is edited. Off
# camera: the clone, a warm build, and the fix's edit, which `git diff` then shows on screen whole.
# The master seed is set off camera (CHARPENTE_SIM_MASTER_SEED) so the render matches README's
# quoted run; the shown command is the bare one a reader types, and every master seed demo.rs
# measured fails the same check.

Output double-charge.gif
Output double-charge.mp4

Set Shell "bash"
Set FontFamily "Menlo"
Set FontSize 22
Set LineHeight 1.25
Set Width 1600
Set Height 900
Set Padding 28
Set Margin 36
Set MarginFill "#1E34B0"
Set WindowBar Colorful
Set BorderRadius 10
Set TypingSpeed 35ms
Set CursorBlink false
Set Theme { "name": "Evening paper", "background": "#ECE3CE", "foreground": "#4A3526", "cursor": "#B5402E", "selection": "#D8CCB0", "black": "#4A3526", "red": "#B5402E", "green": "#5B6B2E", "yellow": "#9A6A12", "blue": "#1E34B0", "magenta": "#4434B6", "cyan": "#2C6B7A", "white": "#ECE3CE", "brightBlack": "#7A6656", "brightRed": "#F2382A", "brightGreen": "#5B6B2E", "brightYellow": "#9A6A12", "brightBlue": "#1E34B0", "brightMagenta": "#4434B6", "brightCyan": "#2C6B7A", "brightWhite": "#ECE3CE" }

Require charpente
Require git
Env BASH_SILENCE_DEPRECATION_WARNING "1"
Env CHARPENTE_SIM_MASTER_SEED "0x434841524745"
Env GIT_PAGER "cat"

Hide
Type `rm -rf /Users/Shared/double-charge && git clone -q --depth 1 https://github.com/NMitchem/charpente /Users/Shared/double-charge && cd /Users/Shared/double-charge/examples/double-charge && cargo test -q --test once --no-run > /dev/null 2>&1 && charpente test --sim --runs 1 > /dev/null 2>&1; echo "setup-$((1+1))-done"`
Enter
# The typed line holds `$((1+1))`, so only the echo's output can match.
Wait+Screen@600s /setup-2-done/
Type `export PS1='\[\e[31m\]$\[\e[0m\] ' && clear`
Enter
Sleep 1s
Show

Sleep 600ms
Type@22ms "# An order API. Its test passes, and it charges some customers twice."
Enter
Sleep 1.5s
Type@22ms "# This route places an order and queues the card charge:"
Enter
Sleep 400ms
Type "grep -A5 'route(POST' src/lib.rs"
Sleep 300ms
Enter
Sleep 3s
Type@22ms "# Its test posts 48 orders and checks that each one is charged once:"
Enter
Sleep 400ms
Type "cargo test --test once"
Sleep 300ms
Enter
Wait+Screen@60s /test result: ok/
Sleep 2.5s

Ctrl+L
Type@22ms "# The simulator checks the same rule over 256 runs, with crashes, disk errors and lost replies:"
Enter
Sleep 400ms
Type "charpente test --sim"
Sleep 300ms
Enter
Wait+Screen@120s /rerun exactly this batch/
Sleep 2.5s
Type@22ms "# A fault hid that order 9's POST succeeded. The client sent it again: two charges."
Enter
Sleep 5s

Ctrl+L
Hide
Type `perl -0pi -e 's/(-> Result<Id<Order>, ApiError> \{\n)/$1    if tx.get(id)?.is_some() {\n        return Ok(id);\n    }\n/; s/use charpente::\{Id, Schema, Tx,/use charpente::{Id, Schema, Tx, View,/' src/lib.rs && charpente test --sim --runs 1 > /dev/null 2>&1; echo "fix-$((1+1))-ready"`
Enter
Wait+Screen@300s /fix-2-ready/
Type "clear"
Enter
Sleep 500ms
Show
Type@22ms "# The fix: answer an order that is already stored."
Enter
Sleep 400ms
Type "git diff -U1"
Sleep 300ms
Enter
Sleep 4s

Ctrl+L
Type@22ms "# Same rule, same 256 runs:"
Enter
Sleep 400ms
Type "charpente test --sim"
Sleep 300ms
Enter
Wait+Screen@120s /sim result: ok/
Sleep 4s
