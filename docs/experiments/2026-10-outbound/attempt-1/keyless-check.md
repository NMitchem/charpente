# Attempt 1 — the ambiguous-call (keyless) check (Task 20 Step 4)

On a copy of the app (`$RUN/keyless`), exactly one line was deleted — the one placing the platform's key on the mailer request:

```diff
diff --git a/src/page.rs b/src/page.rs
index 315f7f8..2561fa4 100644
--- a/src/page.rs
+++ b/src/page.rs
@@ -42,7 +42,6 @@ impl Page {
             .ok_or_else(|| JobError::app(format!("item {} is not in this org", self.item)))?;
         Ok(Some(
             Call::post("/v1/send")
-                .header("idempotency-key", key.as_str())
                 .json(&SendRequest {
                     to: &self.to,
                     subject: format!("Urgent feedback #{}", self.item),
```

Batch: `charpente test --sim --universes 10000 --master-seed 0x9363215d418bc339` in the copy → **EXIT=1, 2,672 failures** (red).

First failure (the agent's own email-exactly-once property):

```json
{
  "test": "urgent_items_page_exactly_once",
  "seed": "0xb2fcbfc15b29ef9f",
  "message": "urgent item 23 of org 3 reads sent, and the mailer sent 2 emails for it: [SentMail { id: \"m-5\", key: \"9f56c384c7a22de1b953f454ad819d34\", to: \"oncall-3-v0@pager.example\", subject: \"Urgent feedback #23\", body: \"[o3 s34 urgent] login fails for all users\" }, SentMail { id: \"m-6\", key: \"9f56c384c7a22de1b953f454ad819d34\", to: \"oncall-3-v0@pager.example\", subject: \"Urgent feedback #23\", body: \"[o3 s34 urgent] login fails for all users\" }]",
  "repro": "charpente test --sim --filter urgent_items_page_exactly_once --seed 0xb2fcbfc15b29ef9f"
}
{
  "outbound_lost_reply": 0.026,
  "outbound_slow": 0,
  "outbound_lost_request": 0
}
```

Config carries a non-zero `outbound_lost_reply` (0.026). Replayed once with the printed repro line → EXIT=1, the same failure text (`$RUN/keyless-repro.log`: `urgent item 23 of org 3 reads sent, and the mailer sent 2 emails for it`, seed 0xb2fcbfc15b29ef9f).

**Check: PASS** — the agent's properties see a double send when the key is removed.
