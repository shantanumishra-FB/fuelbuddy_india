# FuelBuddy India

Moves every Treis Solutions LLP transaction dated on/after 2025-09-29 to FuelBuddy Future Energy Private Limited, in batches
of plain SQL updates (no document is re-saved or re-submitted).

## Install

```
bench get-app <this repo>
bench --site <site> install-app fuelbuddy_india
```

## Run (System Manager)

From the browser console, dry run first, then the move:

```js
frappe.call({method: "fuelbuddy_india.move_to_ffe.start", args: {dry_run: 1}, callback: r => console.log(r.message)})
frappe.call({method: "fuelbuddy_india.move_to_ffe.start", args: {dry_run: 0, keep: 0, batch: 1000}, callback: r => console.log(r.message)})
```

Or `bench --site <site> execute fuelbuddy_india.move_to_ffe.run --kwargs "{'dry_run': 0, 'keep': 0}"`.

| Argument | Default | Meaning |
|---|---|---|
| `dry_run` | 1 | 1 only counts and reports, 0 moves |
| `keep` | 1 | 1 keeps documents tied to pre-cutoff or e-invoiced ones in Treis; 0 moves everything dated on/after the cutoff |
| `batch` | 200 | documents per commit |
| `extra` | `{}` | Treis records whose FFE twin is not named `<name> - <FFE abbr>`, e.g. `{"Bank Account": {"TS HDFC": "FFE HDFC"}}` |

Re-running is safe: it continues with whatever is still on Treis.

## Progress

- Error Log list: `move_to_ffe running: <n> moved, <n> failed`, updated every 10 s; `move_to_ffe finished` holds the summary.
- `frappe.call({method: "fuelbuddy_india.move_to_ffe.status", callback: r => console.log(r.message)})`

## What moves

The 14 main transaction types first (Quotation ... Bank Transaction), then every other submittable doctype with a company and a
date, plus Opportunity and Loyalty Point Entry: header, child rows, GL / payment / stock ledgers and batch bundles. Treis stock at
the cutoff is handed over with a Material Issue / Material Receipt pair and reposted.

Never moved: Period Closing Voucher (redo the year-end closing), Repost Item Valuation and other repost logs, Stock Closing Entry,
Cost Center Allocation, Budget, BOM, Transaction Deletion Record. Documents that use a Treis account, warehouse, cost center,
address or GSTIN with no FFE twin stay until it is mapped in `extra`.
