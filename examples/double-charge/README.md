An order API whose re-sent POST charges the customer twice, and the one command that catches it.
Run `charpente test --sim` in this directory: it goes red with a seed, and the seed replays.
Fix it by answering an order that is already stored (the three lines in `place_order`), and the run goes green.
