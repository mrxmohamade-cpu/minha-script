# How robot scheduling works

## States
- **ACTIVE**: member is eligible to be checked when `next_allowed_check_at` has passed.
- **DISABLED** (terminal): member is completed/beneficiary/not eligible/has RDV/needs pre-inscription and is excluded from scheduling.
- **PAUSED_RATE_LIMIT** (global): when a 429 is detected, the scheduler pauses all checks until the global cooldown expires.

## Result classification
The robot classifies each check result into a `ResultType` based on Arabic status strings and network signals:
- **NO_DATES**: “لا توجد مواعيد”, “تم التحقق”, “تم جلب المعلومات”, “جاري البحث عن مواعيد...”.
- **ERROR_RETRYABLE / NETWORK_ERROR**: network timeouts, connection errors, or “فشل جلب التواريخ”.
- **RATE_LIMIT**: HTTP 429 or server messages indicating too many requests.
- **HAS_DATES**: only when `dates[]` is non-empty (real signal).
- **TERMINAL**: “مكتمل”, “مستفيد حاليا من المنحة”, “غير مؤهل للحجز”, “لديه موعد مسبق”.

## Cooldowns
- **NO_DATES**: 6–24 hours + jitter.
- **ERROR_RETRYABLE / NETWORK_ERROR**: 10–60 minutes + jitter.
- **RATE_LIMIT (429)**: global pause 30–120 minutes + jitter.
- **TERMINAL**: disabled permanently (excluded from scheduling).

## Round behavior
1. Each round checks at most one attempt per member.
2. If a member is cooling down, the scheduler skips it and moves to the next.
3. If all ACTIVE members are cooling down, the robot sleeps until the earliest next-allowed time.
4. If a 429 is detected, the robot pauses globally and stops the round early.

## Logging
Each round logs a decision summary per member (eligible, cooldown, excluded, paused) and the next-allowed time to make scheduling decisions easy to audit.
