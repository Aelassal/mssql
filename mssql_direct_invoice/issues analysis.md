# mssql_direct_invoice — Issues Analysis

Running log of issues discovered during testing. Do not fix until instructed.

---

## RESOLVED

### [R8] CN redemption linkage + cash refund tracking — proper MSSQL mirror
- **Context:** In MSSQL, `tblInvoiceReturnCode` records exactly how each CN was used: `Used=1` + `UsedInvoiceID!=0` means redeemed as PT5 payment on that invoice; `Used=1` + `UsedInvoiceID=0` means cash refund at POS; `Used=0` means outstanding voucher.
- **Old behavior:** Treated PT5 positive amounts as generic "Return Voucher" payments on the session invoice. Result: CNs stayed as unpaid customer credits in Odoo even though they were redeemed in MSSQL.
- **Fix applied:**
  - Added `_query_cn_redemptions()` SQL query
  - Enriched CN data with `Redemption_Used`, `Redemption_UsedInvoiceID`, `Redemption_UsedDate`, `Redemption_Canceled` fields
  - Added `_find_odoo_invoice_by_mssql_id()` helper to locate Odoo invoice by MSSQL invoice ID (checks MSSQL-INV-{id} ref first, then session refs containing the ID)
  - In CN processor: if Used with UsedInvoiceID, find/backfill the redemption invoice and reconcile CN's AR with it; if Used with UsedInvoiceID=0, register cash refund payment on CN; if not Used, leave as customer credit
  - Removed PT5 positive registration from session processor (now handled via CN reconciliation — no double-count)
- **Expected:** CNs show as reconciled when they were redeemed in MSSQL; session invoice residuals = PT5 amounts, which get closed when matching CNs reconcile against them.

### [R7] Credit sales query double-counting STC Pay invoices
- **Context:** MSSQL has no per-invoice column for STC Pay (PT60). Invoices paid via STC Pay show zero in all invoice-level payment columns, so the "credit sales" (unpaid) query misclassifies them as unpaid.
- **Symptom:** Session 12297 decimal target was inflated by +175 (STC Pay invoice), session 12310 by +1038 → session invoices had residuals that should have been 0.
- **Fix applied:** Removed `+ credit_amount` from `mssql_total` calculation. `ca.NetTotal` already includes all sales invoices (paid or unpaid), so adding `credit_amount` was double-counting.

### [R3] Invoice-level discounts lost in per-line aggregation — fixes O1 & O13
- **Fix applied:** Added `_query_all_session_invoice_discounts()` that returns `SUM(tblInvoice.Discount)` per session. Added `_get_or_create_invoice_discount_product()` helper for a "POS Invoice Discount" service product. In the sales processor, after building product lines, add an explicit negative line with this total (tax-inclusive MSSQL Discount → pre-tax via /1.15, with 15% VAT tax_ids).
- **Expected:** Decimal shrinks from tens of SAR to sub-cent for sessions with invoice-level discounts.

### [R4] Decimal line with tax causes 0.01 residual — fixes O10
- **Fix applied:** Decimal line now has `tax_ids: [(6, 0, [])]` (tax-free) and `price_unit = difference` directly (post-tax amount). Applied to sales invoices, purchase bills, and credit notes.
- **Expected:** `amount_residual` = 0 exactly, no sub-cent drift.

### [R5] Credit note partial reconciliation bug — fixes O12
- **Fix applied:** Removed `account.move.reversal` wizard + `refund_moves()` path. Credit notes now always created as standalone `out_refund` with `reversed_entry_id` for linkage. Reconciliation explicitly matches CN's AR line against original's AR line without filtering by `reconciled` state — Odoo breaks prior reconciles as needed.
- **Expected:** CN fully reconciles against original AR; excess stays as legitimate customer credit balance.

### [R6] Pre-sync credit notes with missing originals — fixes O11
- **Fix applied (v1):** In `_process_queue_sales_credit_note`, when original Odoo invoice not found, look up the original's SessionDate from the CN data (enriched during queue creation) and, if within 30-day lookback, call `create_session_based_invoices(orig_date)` to backfill. Retry lookup after backfill.
- **Fix applied (v2 — more efficient):** Replaced the full-session backfill with **single-invoice backfill**. Added `_query_single_invoice_with_details()` and `_backfill_single_invoice(invoice_id)`. Creates a minimal Odoo `out_invoice` with ref `MSSQL-INV-{id}` containing only the specific original invoice's lines + per-invoice payments (from `CashAmount`, `SpanAmount`, `VisaAmount`, `MasterCard`, `CreditAmount`, `ReturnSlip` columns). CN lookup order: session ref → MSSQL-INV ref → backfill.
- **Expected:** CN properly links to its backfilled original without syncing unrelated sales from that session. If original is too old (>30d), CN is created standalone with reversed_entry_id=False.

### [R1] PT5 positive (voucher redemption) not registered as payment
- **Symptom:** Session 12043 had PT5 = +142.50 SAR (customer redeeming a return voucher as payment). The invoice showed residual of 88.49 SAR.
- **Root cause:** PT5 handling was removed during the ZatcaCreditNote rework. The old code appended PT5 as a payment entry when positive.
- **Fix applied:** Re-added `_query_all_session_pt5_returns()`, included `pt5_amount` in queue JSON, and in `_process_queue_sales_invoice()` appended PT5 as a Return Voucher payment when positive.
- **Expected:** Session residual closes to ~0 after credit note reconciliation.

### [R2] Credit sales query incorrectly includes PT6 (On Account) invoices
- **Symptom:** Session 12044 Decimal line = +51.73 SAR (way over threshold). Odoo total = 7088.25 vs MSSQL NetTotal = 7020.75.
- **Root cause:** `_query_all_session_credit_sales()` filters out invoices with cash/bank payments but NOT `CreditAmount`. Session 12044 had 3 "On Account" (PT6) invoices totaling 67.50 SAR. These were counted as "unpaid credit sales" and added to the decimal target, inflating it by 67.50. The PT6 payment (67.50) was already registered separately, so the target should have been just NetTotal.
- **Fix applied:** Added `AND ISNULL(i.CreditAmount, 0) = 0` to `_query_all_session_credit_sales()`.
- **Expected:** Decimal adjustment becomes much smaller (within cents) for sessions with PT6 payments.

---

## OPEN

### [O1] Large decimal adjustment due to aggregated line SubTotal mismatch  — RESOLVED by [R3]
- **Symptom:** Decimal adjustment exceeds ±1 SAR on many sessions even after [R1]/[R2] are applied.
- **Observed cases (post-fix):**

  | Session | Odoo Inv | Line Sum | InvDiscount | NetTotal | Gap | Decimal (pre-tax) |
  |---|---|---|---|---|---|---|
  | 12043 | INV/00031 | 4,447.73 | 3.57 | 4,424.00 | 23.73 | -20.50 |
  | 12044 | INV/00032 | 7,028.79 | 4.97 | 7,020.75 | 8.04 | -6.99 |
  | 12058 | INV/00045 | 5,484.20 | 4.12 | 5,480.00 | 4.20 | -3.67 |
  | 12059 | INV/00046 | 4,750.55 | 2.50 | 4,738.00 | 12.55 | -11.06 |
  | 12060 | INV/00047 | 1,006.50 | 0.25 | 1,006.25 | 0.25 | -0.23 ✓ |

- **Root cause:** The code builds invoice lines from `SUM(tblInvoiceDetail.SubTotal)` grouped by ItemID. Because:
  - Invoice-level discounts (`tblInvoice.Discount`) are NOT reflected in `LineDiscount` — the code's per-line calculation misses them entirely.
  - Aggregation across hundreds of detail lines accumulates sub-cent rounding differences.
  - Some sessions show gaps (12.55 SAR on 12059) larger than the invoice discount (2.50) — indicating MSSQL line SubTotals sometimes don't sum to the invoice's own NetTotal even before invoice-level discount.
- **Possible approaches:**
  - **A.** Use invoice-level totals as target: fetch `SUM(i.NetTotal)` per session (same as `ca.NetTotal` typically) — current target, no change needed there — **but** subtract `SUM(i.Discount)` from the aggregated line sum before tax calc so lines match invoice pre-discount total, letting invoice discount flow through as its own negative line.
  - **B.** Don't aggregate lines by ItemID — create one Odoo line per MSSQL detail line (matches `mssql_invoice_sync`, higher fidelity). Eliminates ItemID-group rounding but still may miss invoice-level discounts.
  - **C.** Add an explicit "Invoice Discount" line using `SUM(i.Discount)` so the decimal only captures sub-cent rounding.
- **Recommended:** Option C combined with B — one line per MSSQL detail line + one explicit invoice-discount line per session. Should bring decimal to ±1 reliably.
- **Decision:** Deferred.

### [O2] Orphan-session credit notes (sessions with NULL SessionDate)
- **Symptom:** On 2026-03-01, `tblZatcaCreditNote` has 14 credit notes but CRA vouchers only link to 6. The other 8 belong to session 12012 (SessionDate = NULL — a dedicated "returns counter" session).
- **Current handling:** The new ZatcaCreditNote-by-date flow (`create_sales_credit_notes`) catches these correctly. However, the linkage to an original Odoo invoice relies on finding the original session via `ReturnInvoiceID → tblInvoice.SessionID` — which may or may not match a synced Odoo invoice depending on when the original sale was synced.
- **Status:** Works correctly for typical flow. Edge cases where the original invoice was never synced fall back to standalone credit note path.
- **Decision:** Monitor in production; no change needed yet.

### [O3] Unposted purchase returns (IsReturn=1, Posted=0)
- **Symptom:** 699 purchase return invoices in MSSQL have `Posted=0` (unposted) and are excluded by the current `AND pi.Posted = 1` filter. Total value: -1,338,716 SAR.
- **Question:** Is this intentional (skip draft returns) or should these be synced as draft `in_refund`?
- **Decision:** Business decision pending. Currently only posted PO returns are imported.

### [O4] Unmapped payment types (PT50, PT100)
- **Symptom:** `tblCashierActivityDetail` has PaymentType=50 (3,027 rows) and PaymentType=100 (114 rows). Not in `tblPaymentType`.
- **Data check:** All occurrences have `PCAmount = 0` — so no actual payments to register.
- **Decision:** No action needed.

### [O5] Vendor payment methods 4 and 5 unmapped
- **Symptom:** `tblSuppliersPayment` has PaymentMethod=4 (691 rows) and PaymentMethod=5 (3,465 rows). Code only maps 1 (Cash), 2 (Check), 3 (Bank Transfer).
- **Data check:** All occurrences have `PaymentAmount = 0` — no actual payments.
- **Decision:** No action needed.

### [O6] Sessions with negative NetTotal and NULL SessionDate (dedicated return counters)
- **Symptom:** 449 sessions in `tblCashierActivity` have `SessionDate = NULL`, `SalesInvoiceCount = 0`, `ReturnInvoiceCount > 0`, negative NetTotal. These are return counter sessions (mostly EmployeeID 35).
- **Status:** Not processed by session-invoice flow (filtered by date). Their credit notes ARE captured by the new ZatcaCreditNote-by-date flow.
- **Decision:** No action needed — correct behavior.

### [O7] Ancient corrupt sessions (2018-2021)
- **Symptom:** Sessions 1-10 span 34,000+ hours (years open). Some have NetTotals like -15 trillion (corrupt data).
- **Decision:** Legacy data, out of sync scope. No action needed.

### [O8] Six regular POs with NetTotal = 0
- **Symptom:** 6 purchase invoices with `IsReturn=0`, `Posted=1`, `NetTotal=0`. Will create empty `in_invoice` records.
- **Decision:** Harmless but noisy. Monitor; add a `NetTotal != 0` filter if it causes issues.

### [O11] Credit notes for original invoices from pre-sync dates leave unreconciled residuals — RESOLVED by [R6]
- **Symptom:** `RINV/2026/00047` (MSSQL-CN-2603040011700003, 66.00 SAR):
  - Original MSSQL invoice 2602260010161700034 belongs to session **11978** (2026-02-26)
  - Sync only started from 2026-03-01, so session 11978 was never imported
  - Credit note created standalone with `reversed_entry_id = NULL`
  - `amount_residual = 66.00` (fully outstanding) — no invoice to reconcile against
- **Impact:** Customer's AR balance shows negative 66 SAR due — misleading since the original sale/payment was made in MSSQL long before Odoo saw it.
- **Possible approaches:**
  - Skip credit notes whose `OriginalSessionID` is earlier than the earliest synced session
  - Offset standalone credit notes against a "prior period balance" account instead of letting them dangle
  - Backfill-sync original invoices needed by credit notes
- **Decision:** Deferred. Flag to user what sync window they want.

### [O12] Credit note partial reconciliation when original invoice is already fully paid — RESOLVED by [R5]
- **Symptom:** `RINV/2026/00053` (48.00 SAR) linked to `INV/2026/00024` (5,958.00, residual 0.00):
  - `reversed_entry_id = 121` ✓ linked correctly
  - Only **18.00** reconciled, **30.00 residual** remains on the credit note
- **Hypothesis:** When the credit note was posted, the original invoice's receivable line was already partially/fully reconciled with payments, so only a partial amount was available to consume. The remaining 30.00 sits as a dangling credit.
- **Investigation needed:** Trace exact reconciliation path to understand what happened. Could be payment timing, or that the original invoice had only 18 SAR of unreconciled receivable when CN was posted (despite showing residual 0 now after reconciliation).
- **Decision:** Deferred, needs more investigation.

### [O13] MSSQL invoices where line SubTotals exceed invoice Total (data anomaly) — RESOLVED by [R3]
- **Symptom:** Session 12053 has two invoices where `SUM(tblInvoiceDetail.SubTotal)` is significantly larger than the invoice's `Total`:
  - `2603040010071700027`: Lines = 505.50, Total = 457.49, Discount = 0.24 — unexplained 48.01 gap
  - `2603040010071700046`: Lines = 1,371.25, Total = 1,361.61, Discount = 0.11 — unexplained 9.75 gap
- **Observation:** These invoices also show duplicated `ItemID` rows with different `UnitPrice` values, suggesting data entry issues (split lines, aborted edits, free items not flagged, etc.).
- **Impact:** Makes [O1] decimal gap much larger than `InvDiscount` alone would explain (session 12053 gap = 62.09 vs InvDiscount = 4.31).
- **Decision:** Can't fix in sync — source data issue. Using invoice-level totals in [O1] fix (approach A or C) would bypass this entirely.

### [O10] Sub-cent residual on some invoices (0.01 SAR) — RESOLVED by [R4]
- **Symptom:** Session 12060 / INV/2026/00047 has `amount_total = 1,006.26` but payments sum to exactly 1,006.25 (matches MSSQL NetTotal). Residual = 0.01.
- **Root cause:** The decimal adjustment's pre-tax value (e.g., -0.2174 → rounded to -0.23) when multiplied by 1.15 tax gives -0.2645, causing the final Odoo total to be off from the target MSSQL NetTotal by a sub-cent amount after rounding to 2 decimals.
- **Decision:** Cosmetic. Either accept (Odoo handles 0.01 residuals as "cash rounding") or tighten decimal calc with a second iteration that verifies post-tax total exactly.

### [O9] Three sales invoices with IsReturned=0 but negative NetTotal
- **Symptom:** Ancient records (2018-2021). Won't appear in any recent sync date.
- **Decision:** No action needed.

---

## NOTES / OBSERVATIONS

- **MSSQL data discrepancies:** `ca.NetTotal` (session-level), `SUM(tblInvoice.NetTotal)`, and `SUM(tblInvoiceDetail.SubTotal)` don't always match exactly in POSDataA. This is a source data issue, not a sync bug.
- **Session 12043 credit note linkage:** `RINV/2026/00043` (54 SAR) was tied to `INV/2026/00031` (session 12043) via the reversal wizard, even though the credit note's MSSQL session was 12040. The link came from the credit note's `ReturnInvoiceID` pointing to an invoice that belonged to session 12043. This behavior is correct but counter-intuitive — worth documenting if confusion arises.
