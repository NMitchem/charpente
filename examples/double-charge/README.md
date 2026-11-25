An order API with no guard for an order that is already stored. Under charpente 0.6.0 a re-sent POST charged the customer twice, and `charpente test --sim` caught it with a seed.
Under 0.7.0 the simulator re-sends each order with the `Idempotency-Key` it first carried, and charpente answers a copy whose first send landed from its record, without placing the order again.
Run `charpente test --sim` in this directory: the same route, with no guard, goes green.
charpente dedupes the re-sends of one write, not two writes: a second POST of the same order under a new key still places it again and charges it twice. The generated client mints a fresh key on each call unless you pass the first call's key to retry it, so a route that must answer a repeated order keeps the guard.
