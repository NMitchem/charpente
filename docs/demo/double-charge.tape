# charpente launch demo: the simulator catches a double charge in examples/double-charge.
# Render: vhs docs/demo/double-charge.tape   (writes double-charge.gif and double-charge.mp4 to the
# current directory; keep renders out of git)
# Every line on screen is real output from charpente 0.6.0 as installed from crates.io
# (`cargo install charpente-cli`), run on a clone of this repository; nothing is edited.
# The frame holds 23 rows of 103 columns; the batch's output takes 19 of them (demo.rs pins it).

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

# Setup, off camera: a fresh clone at a short path, the demo built once so the shown run
# prints at once rather than after a cold build.
Hide
Type `rm -rf /Users/Shared/double-charge && git clone -q --depth 1 https://github.com/NMitchem/charpente /Users/Shared/double-charge && cd /Users/Shared/double-charge/examples/double-charge && charpente test --sim --runs 1 > /dev/null 2>&1; echo "setup-$((1+1))-done"`
Enter
# The typed line holds `$((1+1))`, so only the echo's output can match.
Wait+Screen@600s /setup-2-done/
Type `export PS1='\[\e[31m\]$\[\e[0m\] ' && clear`
Enter
Sleep 1s
Show

Sleep 600ms
Type "# An order API: POST /orders/:id charges a card through a payment gateway."
Enter
Sleep 1.2s
Type "charpente test --sim --master-seed 0x434841524745"
Sleep 400ms
Enter
Wait+Screen@120s /rerun exactly this batch/
Sleep 5s

Ctrl+L
Type "# Same seed, same double charge."
Enter
Sleep 1s
Type@10ms "charpente test --sim --filter each_order_is_charged_once --seed 0x82eac71fbdeda9de --inputs 00 --faults-off crash_torn,object_put_error,object_get_error,outbound_refused,disk_latency,net_latency"
Sleep 400ms
Enter
Wait+Screen@120s /sim result: FAILED/
Sleep 5s
