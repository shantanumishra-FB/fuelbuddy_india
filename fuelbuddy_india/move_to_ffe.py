# One-off move of every transaction dated on/after the cutoff from OLD to NEW: the DOCTYPES below first, then every other submittable
# doctype with a company and a date (see doctypes(); DENY never moves). Set-based: per batch of documents, one UPDATE
# per table and company-specific field (header, child rows, GL / payment / stock ledgers, batch bundles), then one commit.
#   bench --site <site> execute fuelbuddy_india.move_to_ffe.run --kwargs "{'dry_run': 0}"
#   or POST /api/method/fuelbuddy_india.move_to_ffe.start {"dry_run": 0} (System Manager; runs on the long queue)
# Progress: Error Log "move_to_ffe started" (updated after every batch). Result: Error Log "move_to_ffe finished".
import json
import time

import frappe
from frappe.utils import add_days, cint, date_diff, flt, getdate, now_datetime, nowdate

OLD = "Treis Solutions LLP"
NEW = "FuelBuddy Future Energy Private Limited"
CUTOFF = "2025-09-29"
STOCK_ACCOUNT = "Temporary Opening"  # stock hand-over posts against "<this> - ABBR" in each company; "" = no hand-over
DOCTYPES = {  # moved in this order, sources first -> date field compared with the cutoff
	"Quotation": "transaction_date",
	"Sales Order": "transaction_date",
	"Material Request": "transaction_date",
	"Purchase Order": "transaction_date",
	"Purchase Receipt": "posting_date",
	"Delivery Note": "posting_date",
	"Stock Entry": "posting_date",
	"Stock Reconciliation": "posting_date",
	"Purchase Invoice": "posting_date",
	"Landed Cost Voucher": "posting_date",
	"Sales Invoice": "posting_date",
	"Payment Entry": "posting_date",
	"Journal Entry": "posting_date",
	"Bank Transaction": "date",
}
LEDGERS = ("GL Entry", "Payment Ledger Entry", "Advance Payment Ledger Entry", "Stock Ledger Entry")
BIN_DTS = {"Sales Order": "Sales Order Item", "Purchase Order": "Purchase Order Item", "Material Request": "Material Request Item"}
UP = {  # document -> its item table and [link, source]: made from a source that stays in OLD -> it stays too (keep=1)
	"Sales Invoice": ["Sales Invoice Item", ["delivery_note", "Delivery Note"], ["sales_order", "Sales Order"]],
	"Delivery Note": ["Delivery Note Item", ["against_sales_order", "Sales Order"], ["against_sales_invoice", "Sales Invoice"]],
	"Sales Order": ["Sales Order Item", ["prevdoc_docname", "Quotation"]],
	"Purchase Invoice": ["Purchase Invoice Item", ["purchase_receipt", "Purchase Receipt"], ["purchase_order", "Purchase Order"]],
	"Purchase Receipt": ["Purchase Receipt Item", ["purchase_order", "Purchase Order"], ["purchase_invoice", "Purchase Invoice"],
		["material_request", "Material Request"]],
	"Purchase Order": ["Purchase Order Item", ["material_request", "Material Request"], ["sales_order", "Sales Order"]],
	"Material Request": ["Material Request Item", ["sales_order", "Sales Order"]],
	"Stock Entry": ["Stock Entry Detail", ["material_request", "Material Request"]],
}
DYN = {  # document -> child table, doctype field, name field of rows that point at other documents
	"Payment Entry": ["Payment Entry Reference", "reference_doctype", "reference_name"],
	"Journal Entry": ["Journal Entry Account", "reference_type", "reference_name"],
	"Landed Cost Voucher": ["Landed Cost Purchase Receipt", "receipt_document_type", "receipt_document"],
	"Bank Transaction": ["Bank Transaction Payments", "payment_document", "payment_entry"],
}
ADDRESS_DISPLAY = {"dispatch_address_name": "dispatch_address", "source_warehouse_address": "source_address_display",
	"target_warehouse_address": "target_address_display"}
GSTIN_FIELDS = ("company_gstin", "bill_from_gstin", "bill_to_gstin")
CHECKED = {"GL Entry": {"account": "Account", "cost_center": "Cost Center"}, "Payment Ledger Entry": {"account": "Account"},
	"Stock Ledger Entry": {"warehouse": "Warehouse"}}  # moved ledger rows must not keep these OLD records
REPORT = ("Account", "Cost Center", "Warehouse", "Bank Account", "Address", "Sales Taxes and Charges Template",
	"Purchase Taxes and Charges Template", "Item Tax Template")  # OLD records with no NEW twin are listed
KEEP = ("Bank Transaction Rule", "Lead", "Prospect", "Opportunity", "Project", "Task", "Issue", "Warranty Claim")  # company records a
# moved document may keep pointing at (CRM, projects, rules): neither mapped nor a reason to stay
DATE_FIELDS = ("posting_date", "transaction_date", "date", "purchase_date", "report_date", "failure_date", "inst_date", "mntc_date",
	"from_date", "start_date", "planned_start_date", "departure_time", "schedule_date")  # first one a doctype has is its date
DENY = ("Period Closing Voucher", "Repost Item Valuation", "Repost Payment Ledger", "Repost Accounting Ledger", "Stock Closing Entry",
	"Closing Stock Balance", "Process Payment Reconciliation", "Transaction Deletion Record", "Cost Center Allocation", "Budget", "BOM",
	"BOM Creator", "Serial and Batch Bundle", "C-Form")  # never moved: closings, reposts, logs, per-company setup, bundles (move with vouchers)
RECORDS = {"Opportunity": "transaction_date", "Loyalty Point Entry": "posting_date"}  # not submittable, still moved when dated on/after
FOLLOW = {"Asset Depreciation Schedule": ["asset", "Asset"]}  # moved once the document it belongs to has moved
MAP = "move_to_ffe_map"
CHUNK = 50000  # names read per query, so memory stays flat on tables with millions of documents
PROGRESS_EVERY = 10  # seconds between progress updates (Error Log entry + cache); also on every finished doctype
COUNT_LIMIT = 10  # seconds a doctype's total count may take; past it the progress shows no percentage for that doctype


@frappe.whitelist(methods=["POST"])
def start(dry_run=1, batch=200, keep=1, extra=None, old=OLD, new=NEW, cutoff=CUTOFF, stock_account=STOCK_ACCOUNT):
	"""Queue the move on the long queue. dry_run=1 only counts. keep=0 also moves what the earlier script kept in OLD."""
	frappe.only_for("System Manager")
	args = clean_args(dry_run, batch, keep, extra, old, new, cutoff, stock_account)
	job = frappe.enqueue("fuelbuddy_india.move_to_ffe.run", queue="long", timeout=24 * 3600, job_id="move_to_ffe", deduplicate=True, **args)
	return {"queued": bool(job), **args}


@frappe.whitelist()
def status():
	"""How far the running (or last) move got: what it wrote after its last batch."""
	frappe.only_for("System Manager")
	st = frappe.cache.get_value("move_to_ffe:progress") or {"state": "no run seen yet (see Error Log 'move_to_ffe ...')"}
	st["job_running"] = not frappe.db.sql("select is_free_lock('move_to_ffe')")[0][0]
	return st


def run(dry_run=1, batch=200, keep=1, extra=None, old=OLD, new=NEW, cutoff=CUTOFF, stock_account=STOCK_ACCOUNT):
	args = clean_args(dry_run, batch, keep, extra, old, new, cutoff, stock_account)
	if not frappe.db.sql("select get_lock('move_to_ffe', 0)")[0][0]:
		frappe.throw("move_to_ffe is already running")
	try:
		return Move(**args).run()
	except Exception as e:
		st = frappe.cache.get_value("move_to_ffe:progress") or {}
		st.update(state=f"stopped by an error: {str(e)[:300]} (run it again, it continues)")
		frappe.cache.set_value("move_to_ffe:progress", st, expires_in_sec=7 * 86400)
		raise
	finally:
		frappe.db.sql("select release_lock('move_to_ffe')")


def clean_args(dry_run, batch, keep, extra, old, new, cutoff, stock_account):
	if isinstance(extra, str):
		extra = json.loads(extra or "{}")
	extra = extra or {}
	if not isinstance(extra, dict) or not all(isinstance(v, dict) for v in extra.values()):
		frappe.throw('extra must look like {"Bank Account": {"old name": "new name"}}')
	if old == new or not frappe.db.exists("Company", old) or not frappe.db.exists("Company", new):
		frappe.throw("old and new must be two different Company names")
	if not cutoff:
		frappe.throw("move_to_ffe: cutoff is required, e.g. 2025-09-29")
	return {"dry_run": flag(dry_run, 1), "batch": min(max(cint(batch), 1), 5000), "keep": flag(keep, 1), "extra": extra,
		"old": old, "new": new, "cutoff": str(getdate(cutoff)), "stock_account": stock_account or ""}


def flag(v, default):
	"""0 or 1 from 0, 1, true, false, yes, no; anything else is refused, so a typo can't start a real move."""
	if v is None or v == "":
		return default
	s = str(v).strip().lower()
	if s in ("1", "true", "yes"):
		return 1
	if s in ("0", "false", "no"):
		return 0
	frappe.throw(f"move_to_ffe: use 0 or 1, not {v!r}")


def fmt(sec):
	sec = int(sec)
	return f"{sec // 3600} h {sec % 3600 // 60} min" if sec >= 3600 else f"{sec // 60} min {sec % 60} s" if sec >= 60 else f"{sec} s"


class Move:
	def __init__(self, dry_run, batch, keep, extra, old, new, cutoff, stock_account):
		self.dry_run, self.batch, self.keep, self.extra = dry_run, batch, keep, extra
		self.old, self.new, self.cutoff, self.stock_account = old, new, cutoff, stock_account
		self.old_sfx = " - " + frappe.db.get_value("Company", old, "abbr")
		self.new_sfx = " - " + frappe.db.get_value("Company", new, "abbr")
		self.ledgers = [d for d in LEDGERS if frappe.db.table_exists(d)]
		self.plans, self.columns, self.stats, self.kept, self.failed = {}, {}, {}, {}, {}
		self.done = []  # doctypes already moved in this run
		self.stock_pairs, self.bin_pairs, self.pending = set(), set(), [set(), set(), []]
		self.ownco = {}  # "Doctype|name" -> its company, for Dynamic Links
		self.lcv = None
		self.posted = {}
		self.dts = self.doctypes()
		self.log, self.t0 = None, time.time()
		self.taxacc = self.tax_accounts()
		self.again, self.track, self.last_write = {}, 0.0, 0.0  # doctype -> names to retry next pass; tracker seconds

	def run(self):
		self.build_map()
		self.block, self.warn = self.preflight()
		self.log = frappe.log_error(title="move_to_ffe " + ("dry run started" if self.dry_run else "started"),
			message=f"{'counting' if self.dry_run else 'moving'} {self.old} documents dated on/after {self.cutoff} to {self.new}")
		frappe.db.commit()
		for dt in self.dts:
			self.stats[dt] = {"found": 0, "moved": 0, "kept": 0, "failed": 0, "swept": 0, "total": 0}
		self.finished = {}  # doctype -> one line for the progress entry
		self.pos = 0
		for npass in range(3):  # later passes retry only what stayed or failed: it may have waited on a later doctype (asset -> depreciation)
			before = sum(x["moved"] for x in self.stats.values())
			retry, self.again = self.again, {}
			if npass and not retry:
				break
			self.kept, self.failed = {}, {}
			for pos, dt in enumerate(self.dts):
				self.pos = pos  # document types done in this pass, for the progress entry
				only = retry.get(dt) if npass else None
				if npass and not only:
					continue
				total = len(only) if npass else self.count(dt)
				found0 = self.stats[dt]["found"]
				self.stats[dt].update(found=0, kept=0, failed=0, total=total, t0=time.time(), moved0=self.stats[dt]["moved"])
				if total == 0:
					continue
				self.stayed, self.same_refs, after = set(), None, ""
				self.say(f"pass {npass + 1}, {dt}: {total if total is not None else 'not counted'} to look at")
				self.progress(dt, "", force=True)
				k = 0
				while True:  # CHUNK names at a time: in name order, or the retry list
					if only:
						names, after, n = self.names(dt, after, only[k : k + CHUNK])
						k += CHUNK
					else:
						names, after, n = self.names(dt, after)
					if not self.dry_run:
						for i in range(0, len(names), self.batch):
							self.move(dt, names[i : i + self.batch], self.old)
					if (only and k >= len(only)) or (not only and n < CHUNK):
						break
				if not self.dry_run and dt not in self.done:
					self.done.append(dt)
				x = self.stats[dt]
				if npass:  # the retried documents were already counted as found in the first pass
					x["found"] = found0
				self.finished[dt] = (f"{dt}: {x['found']:,} found, " + (f"would move {x['found'] - x['kept']:,}" if self.dry_run else
					f"moved {x['moved']:,}") + f", stay {x['kept']:,}, failed {x['failed']:,} ({fmt(time.time() - x['t0'])})")
				self.progress(dt, "", force=True)
				frappe.db.commit()
			if self.dry_run or sum(x["moved"] for x in self.stats.values()) == before:
				break
		notes = []
		if not self.dry_run:
			self.sweep()
			if self.stock_account and self.block:
				notes = ["stock hand-over not done yet: fix the settings listed above, then run again (it hands over then)"]
			elif self.stock_account:
				notes = self.hand_over()
			notes += self.fix_bins()
		lines = self.summary(notes)
		frappe.log_error(title="move_to_ffe " + ("dry run finished" if self.dry_run else "finished"), message="\n".join(lines))
		self.progress(None, "", state="done")
		frappe.db.commit()
		return lines

	# ---- the map of OLD records to their NEW twins, in a temporary table the UPDATEs join on

	def build_map(self):
		coll = frappe.db.sql("""select collation_name from information_schema.columns where table_schema = database()
			and table_name = 'tabAccount' and column_name = 'name'""")[0][0]
		frappe.db.sql(f"drop temporary table if exists {MAP}")
		frappe.db.sql(f"""create temporary table {MAP} (dt varchar(140) not null, old varchar(255) not null, new varchar(255) not null,
			disp longtext, primary key (dt, old)) character set utf8mb4 collate {coll}""")
		rows = {("Company", self.old): [self.new, None]}
		self.unmapped = {}
		pat = "%" + self.old_sfx.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
		self.master_set = self.masters()
		for t in sorted(self.master_set):
			ex = self.extra.get(t, {})
			cand = {}
			for o in frappe.db.sql_list(f"select name from `tab{t}` where company = %s and name like %s", (self.old, pat)):
				cand[o] = o[: -len(self.old_sfx)] + self.new_sfx
			for o in ex:
				if frappe.db.get_value(t, o, "company") == self.old:
					cand[o] = ex[o]
			news = self.existing(t, list(cand.values()))
			for o, n in cand.items():
				if n in news:
					rows[(t, o)] = [n, None]
			if t in REPORT:
				grp = " and is_group = 0" if "is_group" in self.cols(t) else ""
				for o in frappe.db.sql_list(f"select name from `tab{t}` where company = %s{grp}", self.old):
					if (t, o) not in rows:
						self.unmapped.setdefault(t, []).append(o)
		gst, self.gst_old, gst_new = {}, set(), set()
		for t in ("Address", "Contact"):
			if not frappe.db.table_exists(t):
				continue
			olds, news = (set(frappe.db.sql_list("""select parent from `tabDynamic Link` where parenttype = %s
				and link_doctype = 'Company' and link_name = %s""", (t, co))) for co in (self.old, self.new))
			ex = self.extra.get(t, {})
			if t == "Address" and "gstin" in self.cols("Address"):
				gst_new = set(frappe.db.sql_list("select gstin from `tabAddress` where name in %s and ifnull(gstin, '') != ''", (tuple(news) or ("",),)))
				self.gst_old = set(frappe.db.sql_list("select gstin from `tabAddress` where name in %s and ifnull(gstin, '') != ''",
					(tuple(olds - news) or ("",),))) - gst_new
			for o in sorted(olds - news):
				n = ex.get(o) or (o[: -len(self.old_sfx)] + self.new_sfx if o.endswith(self.old_sfx) else None)
				if n not in news:
					if t in REPORT:
						self.unmapped.setdefault(t, []).append(o)
					continue
				disp = None
				if t == "Address":
					a, b = frappe.get_doc("Address", o), frappe.get_doc("Address", n)
					disp = b.get_display()
					if a.get("gstin") and b.get("gstin") and a.gstin != b.gstin:
						gst.setdefault(a.gstin, b.gstin)
				rows[(t, o)] = [n, disp]
		gst.update(self.extra.get("GSTIN", {}))
		for g, n in gst.items():
			if n and n[:2] == g[:2]:
				rows[("GSTIN", g)] = [n, None]
			else:  # documents keeping this GSTIN stay (no_twin)
				self.unmapped.setdefault("GSTIN", []).append(f"{g} -> {n or 'nothing'} (map it to a {self.new} GSTIN of the same state)")
		vals = [(k[0], k[1], v[0], v[1]) for k, v in rows.items()]
		for i in range(0, len(vals), 500):
			chunk = vals[i : i + 500]
			frappe.db.sql(f"insert into {MAP} (dt, old, new, disp) values " + ", ".join(["(%s, %s, %s, %s)"] * len(chunk)),
				[x for r in chunk for x in r])
		frappe.db.commit()  # the map rows survive the rollback of a failed batch
		self.mapped = {k[0] for k in rows}
		self.owned_dt = self.owned_doctypes()
		self.owned = self.owned_dt | {"Address", "Contact"}
		self.acc = {k[1]: v[0] for k, v in rows.items() if k[0] == "Account"}
		self.wh = {k[1]: v[0] for k, v in rows.items() if k[0] == "Warehouse"}
		self.say(f"map: {len(rows)} {self.old} records have a {self.new} twin")

	def masters(self):
		"""Doctypes the moved tables link to that belong to one company (Account, Cost Center, Warehouse, ...)."""
		out, seen = set(), set()
		for t in self.tables():
			for f in frappe.get_meta(t).fields:
				o = f.options
				if f.fieldtype != "Link" or not o or o in seen or o in DOCTYPES or o in KEEP:
					continue
				seen.add(o)
				if not frappe.db.exists("DocType", o) or not frappe.db.table_exists(o):
					continue
				m = frappe.get_meta(o)
				cf = m.get_field("company")
				if not (m.istable or m.issingle or m.is_submittable or m.get("is_virtual")) and cf and cf.fieldtype == "Link" \
						and cf.options == "Company":
					out.add(o)
		return out

	def owned_doctypes(self):
		"""Every doctype whose records belong to one company (masters and transactions) that this move does not move itself."""
		out = set()
		for p in frappe.db.sql_list("""select parent from `tabDocField` where fieldname = 'company' and fieldtype = 'Link' and options = 'Company'
				union select dt from `tabCustom Field` where fieldname = 'company' and fieldtype = 'Link' and options = 'Company'"""):
			if p in DOCTYPES or p in KEEP or p in LEDGERS or p in ("Company", "Serial and Batch Bundle") or not frappe.db.table_exists(p):
				continue
			m = frappe.get_meta(p)
			if not (m.istable or m.issingle or m.get("is_virtual")):
				out.add(p)
		return out

	def doctypes(self):
		"""Doctype -> date field: DOCTYPES in their order, then every other submittable doctype with a company (and RECORDS, FOLLOW)."""
		out = {dt: f for dt, f in DOCTYPES.items() if frappe.db.table_exists(dt)}
		rest = {}
		for p in frappe.db.sql_list("""select parent from `tabDocField` where fieldname = 'company' and fieldtype = 'Link' and options = 'Company'
				union select dt from `tabCustom Field` where fieldname = 'company' and fieldtype = 'Link' and options = 'Company'"""):
			if p in out or p in DENY or p in LEDGERS or p == "Company" or not frappe.db.table_exists(p):
				continue
			m = frappe.get_meta(p)
			if m.istable or m.issingle or m.get("is_virtual"):
				continue
			if p in FOLLOW:
				rest[p] = None
			elif p in RECORDS:
				rest[p] = RECORDS[p]
			elif m.is_submittable:
				rest[p] = next((f for f in DATE_FIELDS if f in self.cols(p)), "creation")
		return {**out, **{k: rest[k] for k in sorted(rest)}}

	def posts(self, dt, ledgers=None):
		"""Whether dt has rows in the ledgers (GL, payment, stock): their rows move and are checked with it."""
		k = (dt, tuple(ledgers or self.ledgers))
		if k not in self.posted:
			self.posted[k] = any(frappe.db.sql(f"select 1 from `tab{led}` where voucher_type = %s limit 1", dt) for led in k[1])
		return self.posted[k]

	def tables(self):
		out = []
		for dt in self.dts:
			if frappe.db.table_exists(dt):
				out += [dt] + self.children(dt)
		for t in ("Serial and Batch Bundle",):
			if frappe.db.table_exists(t):
				out += [t] + self.children(t)
		return list(dict.fromkeys(out + self.ledgers))

	def existing(self, t, names):
		out = set()
		for i in range(0, len(names), 1000):
			out.update(frappe.db.sql_list(f"select name from `tab{t}` where name in %s and company = %s",
				(tuple(names[i : i + 1000]), self.new)))
		return out

	# ---- which documents move

	def names(self, dt, after, only=None):
		"""Every OLD document of dt on/after the cutoff; with keep=1 the ones the earlier script kept in OLD stay (reasons in SQL)."""
		cols = self.cols(dt)
		done = () if self.dry_run else tuple(self.done)
		v = {"old": self.old, "new": self.new, "cutoff": self.cutoff, "dt": dt, "done": done or ("",), "taxacc": tuple(self.taxacc) or ("",),
			"after": after, "only": tuple(only or ("",))}
		why = []
		if self.keep:
			ei = [c for c in ("irn", "ewaybill") if c in cols]
			if ei:
				why.append(["e-invoice / e-way bill issued under " + self.old, " or ".join(f"ifnull(h.`{c}`, '') != ''" for c in ei)])
			made, same = [], []
			for lf, sdt in UP.get(dt, [None])[1:]:
				if lf in self.cols(UP[dt][0]) and frappe.db.table_exists(sdt):
					made.append(f"""exists(select 1 from `tab{UP[dt][0]}` c join `tab{sdt}` s on s.name = c.`{lf}` where c.parent = h.name
						and c.parenttype = %(dt)s and s.company = %(old)s and s.docstatus < 2
						and (s.`{DOCTYPES[sdt]}` < %(cutoff)s or {frappe.db.escape(sdt)} in %(done)s))""")
			if dt in DYN and frappe.db.table_exists(DYN[dt][0]):
				c, tf, nf = DYN[dt]
				for sdt in DOCTYPES:
					if frappe.db.table_exists(sdt):
						made.append(f"""exists(select 1 from `tab{c}` c join `tab{sdt}` s on s.name = c.`{nf}` where c.parent = h.name
							and c.parenttype = %(dt)s and c.`{tf}` = {frappe.db.escape(sdt)} and s.company = %(old)s and s.docstatus < 2
							and (s.`{DOCTYPES[sdt]}` < %(cutoff)s or {frappe.db.escape(sdt)} in %(done)s))""")
			for f in frappe.get_meta(dt).fields:  # return_against, reversal_of, purchase_receipt, ...: a header link to a moved doctype
				o = f.options
				if f.fieldtype == "Link" and self.dts.get(o) and f.fieldname in cols and f.fieldname != "amended_from" \
						and not f.fieldname.startswith("inter_company"):
					made.append(f"""exists(select 1 from `tab{o}` s where s.name = h.`{f.fieldname}` and s.company = %(old)s
						and s.docstatus < 2 and (s.`{self.dts[o]}` < %(cutoff)s or {frappe.db.escape(o)} in %(done)s))""")
					if o == dt:
						same.append(f.fieldname)
			if dt in ("Purchase Receipt", "Purchase Invoice", "Stock Entry") and self.lcv_receipts(dt):
				v["lcv"] = tuple(self.lcv_receipts(dt))
				made.append("h.name in %(lcv)s")
			if made:
				why.append(["made from, or pointing at, a document that stays in " + self.old, " or ".join(made)])
			if dt in ("Sales Invoice", "Purchase Invoice", "Payment Entry", "Journal Entry") and "Payment Ledger Entry" in self.ledgers:
				why.append(["settles, or is settled by, a document that stays in " + self.old, """exists(select 1
					from `tabPayment Ledger Entry` l join `tabPayment Ledger Entry` o on o.voucher_type = l.against_voucher_type
						and o.voucher_no = l.against_voucher_no and o.against_voucher_no = o.voucher_no and o.delinked = 0
						and o.company = %(old)s and (o.posting_date < %(cutoff)s or o.voucher_type in %(done)s)
					where l.voucher_type = %(dt)s and l.voucher_no = h.name and l.delinked = 0 and l.against_voucher_no != l.voucher_no)
					or exists(select 1 from `tabPayment Ledger Entry` p where p.against_voucher_type = %(dt)s
						and p.against_voucher_no = h.name and p.delinked = 0 and p.voucher_no != p.against_voucher_no
						and p.company = %(old)s and (p.posting_date < %(cutoff)s or p.voucher_type in %(done)s
							or exists(select 1 from `tabPayment Entry` v where p.voucher_type = 'Payment Entry' and v.name = p.voucher_no
								and v.posting_date < %(cutoff)s)
							or exists(select 1 from `tabJournal Entry` v where p.voucher_type = 'Journal Entry' and v.name = p.voucher_no
								and v.posting_date < %(cutoff)s)))"""])
			if dt in ("Sales Order", "Purchase Order") and "Advance Payment Ledger Entry" in self.ledgers:
				why.append(["paid in advance by a document that stays in " + self.old, """exists(select 1
					from `tabAdvance Payment Ledger Entry` p where p.against_voucher_type = %(dt)s and p.against_voucher_no = h.name
						and p.delinked = 0 and p.company = %(old)s
						and (exists(select 1 from `tabPayment Entry` v where p.voucher_type = 'Payment Entry' and v.name = p.voucher_no
								and v.posting_date < %(cutoff)s)
							or exists(select 1 from `tabJournal Entry` v where p.voucher_type = 'Journal Entry' and v.name = p.voucher_no
								and v.posting_date < %(cutoff)s)))"""])
			if dt in ("Purchase Receipt", "Purchase Invoice") and frappe.db.table_exists("Asset"):
				af = "purchase_receipt" if dt == "Purchase Receipt" else "purchase_invoice"
				if af in self.cols("Asset"):
					why.append(["a submitted Asset was bought on it: cancel the Asset, re-create it in " + self.new + " after the move",
						f"exists(select 1 from `tabAsset` a where a.`{af}` = h.name and a.docstatus = 1 and a.company = %(old)s)"])
			ic = [f"h.`represents_company` = %(new)s"] if "represents_company" in cols else []
			for f in frappe.get_meta(dt).fields:
				if f.fieldname.startswith("inter_company") and f.fieldtype == "Link" and f.options in DOCTYPES and f.fieldname in cols:
					ic.append(f"exists(select 1 from `tab{f.options}` s where s.name = h.`{f.fieldname}` and s.company = %(new)s)")
			if ic:
				why.append(["inter-company document with " + self.new + ": settle it by hand", " or ".join(ic)])
			if dt == "Journal Entry" and self.taxacc:
				why.append(["GST / TDS payment or set-off journal, it may settle " + self.old + "'s earlier periods", """exists(select 1
					from `tabJournal Entry Account` a where a.parent = h.name and a.debit > 0 and a.account in %(taxacc)s)
					and not exists(select 1 from `tabJournal Entry Account` a where a.parent = h.name
						and (ifnull(a.party, '') != '' or ifnull(a.reference_name, '') != ''))"""])
		if not self.keep:
			same = []
		sel = "".join(f", ({w[1]})" for w in why) + "".join(f", h.`{x}`" for x in same)
		if dt in FOLLOW:
			lf, pdt = FOLLOW[dt]
			pick = "h.name in %(only)s" if only else f"h.name > %(after)s order by h.name limit {CHUNK}"
			rows = frappe.db.sql(f"""select h.name from `tab{dt}` h join `tab{pdt}` p on p.name = h.`{lf}` where h.company = %(old)s
				and p.company = %(new)s and {pick}""", v)
			why = []
		else:
			pick = "h.name in %(only)s" if only else f"h.name > %(after)s order by h.name limit {CHUNK}"
			rows = frappe.db.sql(f"""select h.name{sel} from `tab{dt}` h where h.company = %(old)s and h.`{self.dts[dt]}` >= %(cutoff)s
				and {pick}""", v)
		refs, stay = {}, {}
		for r in rows:
			reasons = [why[i][0] for i in range(len(why)) if r[i + 1]]
			if reasons:
				stay[r[0]] = reasons[0]
			x = [x for x in r[len(why) + 1 :] if x]
			if x:
				refs[r[0]] = x
		if self.keep and dt in DYN and frappe.db.table_exists(DYN[dt][0]):  # rows pointing at another document of the same doctype
			if self.same_refs is None:
				self.same_refs = {}
				c, tf, nf = DYN[dt]
				for parent, target in frappe.db.sql(f"""select c.parent, c.`{nf}` from `tab{c}` c where c.parenttype = %s and c.`{tf}` = %s
						and ifnull(c.`{nf}`, '') != ''""", (dt, dt)):
					self.same_refs.setdefault(parent, []).append(target)
			for r in rows:
				if r[0] in self.same_refs:
					refs.setdefault(r[0], []).extend(self.same_refs[r[0]])
		grew = bool(stay) or bool(self.stayed)
		while grew:  # a return, reversal or reference to a document that stays keeps its own document too
			grew = False
			for name in refs:
				if name not in stay and any(x in stay or x in self.stayed for x in refs[name]):
					stay[name] = "made from, or pointing at, a document that stays in " + self.old
					grew = True
		out = []
		for r in rows:
			if r[0] in stay:
				self.kept.setdefault(stay[r[0]], []).append(dt + " " + r[0])
				self.again.setdefault(dt, []).append(r[0])
				self.stayed.add(r[0])
			else:
				out.append(r[0])
		self.stats[dt]["found"] += len(rows)
		self.stats[dt]["kept"] += len(rows) - len(out)
		return out, (rows[-1][0] if rows else after), len(rows)

	def lcv_receipts(self, dt):
		"""Receipts covered by a submitted OLD Landed Cost Voucher that stays (dated before the cutoff, or covering a receipt before it)."""
		if self.lcv is None:
			self.lcv = set()
			by = {}
			if frappe.db.table_exists("Landed Cost Purchase Receipt"):
				for lcv, d, rt, rn in frappe.db.sql("""select l.name, l.posting_date, c.receipt_document_type, c.receipt_document
						from `tabLanded Cost Voucher` l join `tabLanded Cost Purchase Receipt` c on c.parent = l.name
						where l.company = %s and l.docstatus = 1""", self.old):
					by.setdefault(lcv, [d, []])[1].append([rt, rn])
			for d, recs in by.values():
				early = getdate(d) < getdate(self.cutoff)
				for rt, rn in recs:
					rd = frappe.db.get_value(rt, rn, DOCTYPES[rt]) if rt in DOCTYPES else None
					early = early or bool(rd and getdate(rd) < getdate(self.cutoff))
				if early:
					self.lcv.update(rt + "|" + rn for rt, rn in recs)
		return [k.split("|", 1)[1] for k in self.lcv if k.split("|", 1)[0] == dt]

	def preflight(self):
		"""Settings that would refuse the stock hand-over (block) or hold back the reposts (warn). Reads settings only, never documents."""
		block, warn, edge = [], [], getdate(add_days(self.cutoff, -1))  # the hand-over posts the day before the cutoff
		for co in (self.old, self.new):
			if frappe.get_meta("Company").has_field("accounts_frozen_till_date"):
				frz = frappe.db.get_value("Company", co, "accounts_frozen_till_date")
			else:
				frz = frappe.db.get_single_value("Accounts Settings", "acc_frozen_upto")
			if frz and getdate(frz) >= edge:
				block.append(f"{co} accounts frozen till {frz}")
		frz = frappe.db.get_single_value("Stock Settings", "stock_frozen_upto")
		if frz and getdate(frz) >= edge:
			block.append(f"stock frozen till {frz}")
		frz = cint(frappe.db.get_single_value("Stock Settings", "stock_frozen_upto_days"))
		if frz and date_diff(nowdate(), edge) >= frz:
			block.append(f"stock entries older than {frz} days are frozen")
		pcv_end = "period_end_date" if frappe.get_meta("Period Closing Voucher").has_field("period_end_date") else "posting_date"
		for co in (self.old, self.new):
			for ap in frappe.get_all("Accounting Period", filters={"company": co, "end_date": [">=", edge]}, pluck="name"):
				for cdt in frappe.get_all("Closed Document", filters={"parent": ap, "parenttype": "Accounting Period", "closed": 1},
						pluck="document_type"):
					if cdt in DOCTYPES:
						block.append(f"{co} accounting period {ap} is closed for {cdt}")
			for pcv in frappe.get_all("Period Closing Voucher", filters={"company": co, "docstatus": 1, pcv_end: [">=", edge]}, pluck="name"):
				block.append(f"{co} closed its books with Period Closing Voucher {pcv} (cancel it, close the year again after the reposts)")
			for sce_dt in ("Stock Closing Entry", "Closing Stock Balance"):
				if frappe.db.exists("DocType", sce_dt):
					for sce in frappe.get_all(sce_dt, filters={"company": co, "status": "Completed", "to_date": [">=", edge]}, pluck="name"):
						block.append(f"{co} has {sce_dt} {sce} (cancel it, make it again after the reposts)")
			fy_day = edge  # the hand-over day and every later year up to today: the reposts post in them
			while fy_day:
				fy_ok, fy_out, fy_end = 0, [], None
				for fy in frappe.get_all("Fiscal Year", filters={"year_start_date": ["<=", fy_day], "year_end_date": [">=", fy_day],
						"disabled": 0}, fields=["name", "year_end_date"]):
					fyc = frappe.get_all("Fiscal Year Company", filters={"parent": fy.name}, pluck="company")
					if not fyc or co in fyc:
						fy_ok = 1
					else:
						fy_out.append(fy.name)
					if not fy_end or fy.year_end_date < fy_end:
						fy_end = fy.year_end_date
				need = not fy_ok and (co == self.new or fy_day == edge or frappe.db.exists("GL Entry", {"company": co, "is_cancelled": 0,
					"posting_date": [">=", fy_day]}))
				if need and fy_out:
					block.append(f"Fiscal Year {', '.join(fy_out)} leaves {co} out of its Companies table (add it, or extend {co}'s own "
						f"year to cover {fy_day})")
				elif need:
					block.append(f"no enabled Fiscal Year covers {fy_day} for {co}")
				fy_day = getdate(add_days(fy_end, 1)) if fy_end and getdate(fy_end) < getdate(nowdate()) else None
		if not self.stock_account:
			return block, warn
		role = frappe.db.get_single_value("Stock Settings", "role_allowed_to_create_edit_back_dated_transactions")
		if role and frappe.db.exists("Has Role", {"parenttype": "User", "role": role}) and not frappe.db.exists("Has Role",
				{"parenttype": "User", "role": role, "parent": frappe.session.user}):
			block.append(f"Stock Settings lets only role {role} post back-dated stock (give it to {frappe.session.user} or clear the setting)")
		from frappe.utils.scheduler import get_scheduler_status

		if (get_scheduler_status() or {}).get("status") != "active":
			warn.append("the scheduler is off or paused (System Settings, or pause_scheduler / maintenance_mode in site config)")
		job = "erpnext.stock.doctype.repost_item_valuation.repost_item_valuation.repost_entries"
		if frappe.get_meta("Stock Reposting Settings").has_field("enable_parallel_reposting") and frappe.db.get_single_value(
				"Stock Reposting Settings", "enable_parallel_reposting"):
			job = "erpnext.stock.doctype.repost_item_valuation.repost_item_valuation.run_parallel_reposting"
		if frappe.db.get_value("Scheduled Job Type", {"method": job}, "stopped"):
			warn.append(f"Scheduled Job Type {job} is stopped (start it)")
		for co in (self.old, self.new):
			an = self.stock_account + " - " + frappe.db.get_value("Company", co, "abbr")
			a = frappe.db.get_value("Account", an, ["company", "is_group", "disabled", "freeze_account", "account_type"], as_dict=True)
			if not a or a.company != co or a.is_group or a.disabled or a.freeze_account == "Yes" or a.account_type == "Stock":
				block.append(f"hand-over account {an} must exist in {co}, enabled, unfrozen, not a group and not of type Stock")
			for dim in frappe.get_all("Accounting Dimension Detail", filters={"company": co, "mandatory_for_bs": 1}, pluck="parent"):
				if not frappe.db.get_value("Accounting Dimension", dim, "disabled"):
					block.append(f"{co} makes accounting dimension {dim} mandatory on balance-sheet entries, which the hand-over can't fill")
		for r in frappe.get_all("Applicable On Account", filters={"is_mandatory": 1}, fields=["parent", "applicable_on_account"]):
			f = frappe.db.get_value("Accounting Dimension Filter", r.parent, ["disabled", "accounting_dimension"], as_dict=True)
			a = frappe.db.get_value("Account", r.applicable_on_account, ["company", "account_type", "is_group"], as_dict=True)
			if f and a and not f.disabled and f.accounting_dimension != "Cost Center" and a.company in (self.old, self.new) \
					and not a.is_group and (a.account_type == "Stock" or r.applicable_on_account == self.stock_account + " - "
					+ frappe.db.get_value("Company", a.company, "abbr")):
				msg = f"Accounting Dimension Filter {r.parent} makes {f.accounting_dimension} mandatory on accounts the hand-over posts to"
				if msg not in block:
					block.append(msg)
		idf = {"reqd": 1}
		if frappe.get_meta("Inventory Dimension").has_field("disabled"):
			idf["disabled"] = 0
		for d in frappe.get_all("Inventory Dimension", filters=idf, pluck="name"):
			block.append(f"Inventory Dimension {d} is mandatory, which the hand-over can't fill")
		bad = {}  # items and warehouses a hand-over Stock Entry refuses
		for i in frappe.get_all("Item", filters={"has_batch_no": 0, "has_serial_no": 0},
				or_filters={"disabled": 1, "end_of_life": ["between", ["1900-01-01", nowdate()]]}, pluck="name"):
			bad["Item|" + i] = f"item {i} is disabled or at its End of Life but holds stock on {edge}"
		for w in frappe.get_all("Warehouse", or_filters={"disabled": 1, "is_group": 1}, fields=["name", "is_group"]):
			bad["Warehouse|" + w.name] = "group" if w.is_group else f"warehouse {w.name} is disabled but takes part in the stock hand-over"
		if bad:
			for item, wh in frappe.db.sql("""select b.item_code, b.warehouse from `tabBin` b join `tabWarehouse` w on w.name = b.warehouse
					where w.company = %s""", self.old):
				hit = []
				for k in ("Item|" + item, "Warehouse|" + wh, "Warehouse|" + str(self.wh.get(wh))):
					if bad.get(k) == "group":
						hit.append(f"{wh} maps to group warehouse {k[10:]} (map it to a non-group {self.new} warehouse in extra)")
					elif bad.get(k):
						hit.append(bad[k])
				if hit and flt(self.last_qty(item, wh)) > 0 and not (frappe.db.get_value("Item", item, "has_batch_no")
						or frappe.db.get_value("Item", item, "has_serial_no")):
					block += [h for h in hit if h not in block]
		return block, warn

	def tax_accounts(self):
		out = set()
		if frappe.db.table_exists("GST Account"):
			for g in frappe.db.sql("select * from `tabGST Account` where company = %s", self.old, as_dict=True):
				if g.get("account_type") in ("Output", "Purchase Reverse Charge"):
					out.update(g[k] for k in g if k.endswith("_account") and g[k])
		if frappe.db.table_exists("Tax Withholding Account"):
			out.update(frappe.db.sql_list("select account from `tabTax Withholding Account` where company = %s and ifnull(account, '') != ''",
				self.old))
		return out

	# ---- moving a batch

	def move(self, dt, names, co, stat="moved"):
		try:
			moved = self.move_batch(dt, names, co)
			self.stats[dt][stat] += len(moved)
			try:
				self.progress(dt, names[-1])
				frappe.db.commit()
			except Exception:
				self.stats[dt][stat] -= len(moved)
				raise
		except Exception:
			frappe.db.rollback()
			self.pending = [set(), set(), []]
			if len(names) > 1:  # redo the batch one document at a time, so only the bad one fails
				for n in names:
					self.move(dt, [n], co, stat)
				return
			self.failed[dt + "|" + names[0]] = 1
			self.again.setdefault(dt, []).append(names[0])
			self.stats[dt]["failed"] += 1
			frappe.log_error(title=f"move_to_ffe failed {dt} {names[0]}", message=frappe.get_traceback(),
				reference_doctype=dt, reference_name=names[0])
			frappe.db.commit()
			return
		self.stock_pairs |= self.pending[0]
		self.bin_pairs |= self.pending[0] | self.pending[1]
		for reason, kdt, n in self.pending[2]:
			self.kept.setdefault(reason, []).append(kdt + " " + n)
			self.again.setdefault(kdt, []).append(n)
			self.stats[dt]["kept"] += 1
		self.pending = [set(), set(), []]

	def move_batch(self, dt, names, co):
		v = {"names": tuple(names), "co": co, "dt": dt, "old": self.old, "now": now_datetime()}
		locked = frappe.db.sql_list(f"select name from `tab{dt}` where name in %(names)s and company = %(co)s for update", v)
		if not locked:
			return []
		v["names"] = tuple(locked)
		stay = self.no_twin(dt, v)
		if stay:
			locked = [n for n in locked if n not in stay]
			for n, reason in stay.items():
				self.pending[2].append([reason, dt, n])
			if not locked:
				return []
			v["names"] = tuple(locked)
		bundles = set()
		if self.posts(dt, ["Stock Ledger Entry"]):
			bundles = self.bundles(dt, v)
			if "Stock Ledger Entry" in self.ledgers:
				for item, wh in frappe.db.sql("""select distinct item_code, warehouse from `tabStock Ledger Entry`
						where voucher_type = %(dt)s and voucher_no in %(names)s and company = %(old)s""", v):
					self.pending[0].add(item + "|" + wh)
		if dt in BIN_DTS and "warehouse" in self.cols(BIN_DTS[dt]):
			for item, wh in frappe.db.sql(f"""select distinct item_code, warehouse from `tab{BIN_DTS[dt]}` where parent in %(names)s
					and parenttype = %(dt)s and ifnull(warehouse, '') != ''""", v):
				self.pending[1].add(item + "|" + wh)
		self.apply(dt, "t.name in %(names)s", v)
		for c in self.children(dt):
			self.apply(c, "t.parent in %(names)s and t.parenttype = %(dt)s", v)
		if self.posts(dt):
			for led in self.ledgers:
				self.apply(led, "t.voucher_type = %(dt)s and t.voucher_no in %(names)s", v)
		if bundles:
			bv = {"names": tuple(bundles), "dt": "Serial and Batch Bundle"}
			self.apply("Serial and Batch Bundle", "t.name in %(names)s", bv)
			for c in self.children("Serial and Batch Bundle"):
				self.apply(c, "t.parent in %(names)s and t.parenttype = %(dt)s", bv)
		frappe.db.sql(f"update `tab{dt}` set modified = %(now)s where name in %(names)s and docstatus = 0", v)  # open forms reload
		if self.posts(dt):
			self.check(dt, v)
		return locked

	def no_twin(self, dt, v):
		"""Documents of the batch that use an OLD record (account, warehouse, project, address, ...) with no NEW twin: they stay."""
		out = {}
		parts = [[dt, "t.name", "t.name in %(names)s"]] + [[c, "t.parent", "t.parent in %(names)s and t.parenttype = %(dt)s"]
			for c in self.children(dt)]
		if self.posts(dt):
			parts += [[led, "t.voucher_no", "t.voucher_type = %(dt)s and t.voucher_no in %(names)s"] for led in self.ledgers]
		for t, doc, key in parts:
			for fn, opt, dyn in self.plan(t)[3]:
				if dyn == 1:
					for n, tdt, val in frappe.db.sql(f"""select distinct {doc}, t.`{opt}`, t.`{fn}` from `tab{t}` t left join {MAP} m
							on m.dt = t.`{opt}` and m.old = t.`{fn}` where {key} and m.old is null and t.`{opt}` in %(owned)s
							and ifnull(t.`{fn}`, '') != ''""", {**v, "owned": tuple(self.owned_dt) or ("",)}):
						k = tdt + "|" + val
						if k not in self.ownco:
							self.ownco[k] = frappe.db.get_value(tdt, val, "company")
						if self.ownco[k] == self.old:
							out.setdefault(n, self.twin_reason(tdt, val))
				elif dyn == 2:
					if self.gst_old:
						for n, val in frappe.db.sql(f"""select distinct {doc}, t.`{fn}` from `tab{t}` t left join {MAP} m
								on m.dt = 'GSTIN' and m.old = t.`{fn}` where {key} and m.old is null and t.`{fn}` in %(gst_old)s""",
								{**v, "gst_old": tuple(self.gst_old)}):
							out.setdefault(n, f"company GSTIN {val} belongs to {self.old}: map its company address (or the GSTIN) in extra")
				elif opt in ("Address", "Contact"):
					for n, val in frappe.db.sql(f"""select distinct {doc}, t.`{fn}` from `tab{t}` t
							join `tabDynamic Link` x on x.parent = t.`{fn}` and x.parenttype = {frappe.db.escape(opt)}
								and x.link_doctype = 'Company' and x.link_name = %(old)s
							left join {MAP} m on m.dt = {frappe.db.escape(opt)} and m.old = t.`{fn}`
							where {key} and m.old is null and not exists(select 1 from `tabDynamic Link` y where y.parent = t.`{fn}`
								and y.parenttype = {frappe.db.escape(opt)} and y.link_doctype = 'Company' and y.link_name = %(new)s)""",
							{**v, "new": self.new}):
						out.setdefault(n, self.twin_reason(opt, val))
				else:
					for n, val in frappe.db.sql(f"""select distinct {doc}, t.`{fn}` from `tab{t}` t join `tab{opt}` x on x.name = t.`{fn}`
							left join {MAP} m on m.dt = {frappe.db.escape(opt)} and m.old = t.`{fn}`
							where {key} and x.company = %(old)s and m.old is null""", v):
						out.setdefault(n, self.twin_reason(opt, val))
		return out

	def twin_reason(self, t, val):
		if frappe.get_meta(t).is_submittable:
			return f"links to {t} {val}, which stays in {self.old}: settle it by hand"
		return f"uses {t} {val} of {self.old}, which has no {self.new} twin: map it in extra"

	def bundles(self, dt, v):
		out = set()
		for t in [dt] + self.children(dt):
			key = "t.name in %(names)s" if t == dt else "t.parent in %(names)s and t.parenttype = %(dt)s"
			for bf in self.plan(t)[2]:
				out.update(frappe.db.sql_list(f"select distinct t.`{bf}` from `tab{t}` t where {key} and ifnull(t.`{bf}`, '') != ''", v))
		if "Stock Ledger Entry" in self.ledgers:
			out.update(frappe.db.sql_list("""select distinct serial_and_batch_bundle from `tabStock Ledger Entry`
				where voucher_type = %(dt)s and voucher_no in %(names)s and ifnull(serial_and_batch_bundle, '') != ''""", v))
		return out

	def apply(self, t, key, v):
		stmts, itr = self.plan(t)[:2]
		for s in stmts:
			frappe.db.sql(s.replace("{key}", key), v)
		if itr and self.acc:  # item_tax_rate is JSON keyed by account
			for val in frappe.db.sql_list(f"""select distinct t.item_tax_rate from `tab{t}` t where {key}
					and ifnull(t.item_tax_rate, '') not in ('', '{{}}')""", v):
				try:
					d = json.loads(val)
				except ValueError:
					continue
				nd = {self.acc.get(k, k): d[k] for k in d}
				if nd != d:
					frappe.db.sql(f"update `tab{t}` t set t.item_tax_rate = %(itr_new)s where {key} and t.item_tax_rate = %(itr_old)s",
						{**v, "itr_new": json.dumps(nd), "itr_old": val})

	def plan(self, t):
		"""UPDATE statements for one table: every Link / Dynamic Link to a mapped record, the fields fetched from it, GSTINs."""
		if t in self.plans:
			return self.plans[t]
		meta, cols = frappe.get_meta(t), self.cols(t)
		fetched = {}
		for f in meta.fields:
			if f.fetch_from and "." in f.fetch_from and f.fieldname in cols and not f.get("is_virtual"):
				lf, src = f.fetch_from.split(".", 1)
				fetched.setdefault(lf, []).append([f.fieldname, src])
		stmts, bundles, owned = [], [], []
		for f in meta.fields:
			fn = f.fieldname
			if fn not in cols or f.get("is_virtual"):
				continue
			if f.fieldtype == "Link" and f.options in self.owned:
				owned.append([fn, f.options, 0])
			elif f.fieldtype == "Dynamic Link" and f.options in cols:
				owned.append([fn, f.options, 1])
			elif fn in GSTIN_FIELDS:
				owned.append([fn, "GSTIN", 2])
			if f.fieldtype == "Link" and f.options == "Serial and Batch Bundle":
				bundles.append(fn)
			if f.fieldtype == "Link" and f.options in self.mapped:
				joins, sets = "", [f"t.`{fn}` = m.new"]
				src_cols = self.cols(f.options) if fetched.get(fn) and frappe.db.table_exists(f.options) else set()
				for ff, src in fetched.get(fn, []):
					if src in src_cols:
						joins = (f" left join `tab{f.options}` so on so.name = m.old left join `tab{f.options}` sn on sn.name = m.new")
						sets.append(f"t.`{ff}` = if(sn.name is not null and ifnull(t.`{ff}`, '') = ifnull(so.`{src}`, ''), sn.`{src}`, t.`{ff}`)")
				if f.options == "Address":
					disp = ADDRESS_DISPLAY.get(fn, fn + "_display")
					df = meta.get_field(disp)
					if disp in cols and df and not df.get("is_virtual"):
						sets.append(f"t.`{disp}` = ifnull(m.disp, t.`{disp}`)")
				stmts.append(f"update `tab{t}` t join {MAP} m on m.dt = {frappe.db.escape(f.options)} and m.old = t.`{fn}`{joins}"
					f" set {', '.join(sets)} where {{key}}")
			elif f.fieldtype == "Dynamic Link" and f.options in cols:
				stmts.append(f"update `tab{t}` t join {MAP} m on m.dt = t.`{f.options}` and m.old = t.`{fn}` set t.`{fn}` = m.new"
					" where {key}")
		for gf in GSTIN_FIELDS:
			if gf in cols:
				stmts.append(f"update `tab{t}` t join {MAP} m on m.dt = 'GSTIN' and m.old = t.`{gf}` set t.`{gf}` = m.new where {{key}}")
		self.plans[t] = (stmts, "item_tax_rate" in cols, bundles, owned)
		return self.plans[t]

	def check(self, dt, v):
		"""Moved ledger rows must not keep an OLD account, cost center or warehouse (it had no NEW twin)."""
		bad = []
		for led, fields in CHECKED.items():
			if led in self.ledgers:
				for f, mdt in fields.items():
					for name, val in frappe.db.sql(f"""select distinct t.voucher_no, t.`{f}` from `tab{led}` t join `tab{mdt}` m on m.name = t.`{f}`
							where t.voucher_type = %(dt)s and t.voucher_no in %(names)s and m.company = %(old)s limit 20""", v):
						bad.append(f"{led} {f} {val} belongs to {self.old} and has no {self.new} twin: map it in extra")
		if bad:
			frappe.throw("; ".join(sorted(set(bad))))

	def sweep(self):
		"""GL rows still on OLD for documents already on NEW (a submit or repost that raced a batch): move those documents again."""
		if "GL Entry" not in self.ledgers:
			return
		for dt in self.dts:
			if not self.posts(dt, ["GL Entry"]):
				continue
			names = frappe.db.sql_list(f"""select distinct g.voucher_no from `tabGL Entry` g join `tab{dt}` h on h.name = g.voucher_no
				where g.voucher_type = %(dt)s and g.company = %(old)s and g.posting_date >= %(cutoff)s and h.company = %(new)s""",
				{"dt": dt, "old": self.old, "new": self.new, "cutoff": self.cutoff})
			for i in range(0, len(names), self.batch):
				self.move(dt, names[i : i + self.batch], self.new, "swept")

	# ---- stock: OLD hands its stock at the cutoff to NEW, then balances are reposted

	def hand_over(self):
		notes = []
		acc = {self.old: self.stock_account + self.old_sfx, self.new: self.stock_account + self.new_sfx}
		if not frappe.db.exists("Account", acc[self.old]) or not frappe.db.exists("Account", acc[self.new]):
			return ["stock not handed over: account " + acc[self.old] + " or " + acc[self.new] + " missing"]
		cc = {co: frappe.db.get_value("Company", co, "cost_center") for co in (self.old, self.new)}
		posting = add_days(self.cutoff, -1)
		pairs = set(self.stock_pairs)
		for item, wh in frappe.db.sql("""select b.item_code, b.warehouse from `tabBin` b join `tabWarehouse` w on w.name = b.warehouse
				where w.company = %s""", self.old):
			pairs.add(item + "|" + wh)
		for p in sorted(pairs):
			item, w_old = p.split("|", 1)
			w_new = self.wh.get(w_old)
			last = self.last_qty(item, w_old)
			q = flt(last)  # includes earlier hand-overs: q - need is what is still owed
			if not w_new:
				if q or p in self.stock_pairs:
					notes.append(f"{item} in {w_old}: no {self.new} warehouse (map it in extra)")
				continue
			q_new = flt(self.last_qty(item, w_new))
			old_ev, new_ev, waiting, reco, late = [], [], 0, 0, 0
			for e in frappe.get_all("Stock Ledger Entry", filters={"item_code": item, "warehouse": ["in", [w_old, w_new]],
					"is_cancelled": 0, "posting_date": [">=", self.cutoff]}, fields=["name", "voucher_type", "voucher_no", "company",
					"actual_qty"], order_by="posting_date asc, posting_time asc, creation asc"):
				ev = [e.voucher_type == "Stock Reconciliation" and not e.actual_qty, flt(e.actual_qty), e.name]
				if e.voucher_type == "Stock Reconciliation":
					reco = 1
				if e.company == self.new:
					new_ev.append(ev)
				else:
					old_ev.append(ev)
					late = late or reco
					waiting += (e.voucher_type + "|" + e.voucher_no) in self.failed
			if waiting:
				notes.append(f"{item} in {w_old}: hand-over waits for {waiting} stock documents that failed to move, run again")
				continue
			if q and (frappe.db.get_value("Item", item, "has_batch_no") or frappe.db.get_value("Item", item, "has_serial_no")):
				notes.append(f"{item} in {w_old}: batch / serial item, hand over {q} by hand")
				continue
			need, bal = 0, 0
			for ev in old_ev:  # a Stock Reconciliation resets the balance, nothing after it depends on the hand-over
				if ev[0]:
					break
				bal += ev[1]
				need = max(need, -bal)
			qty = flt(q - need, 6)
			hold = reco and old_ev and (late or abs(need + bal) > 0.000001)
			if hold:
				notes.append(f"{item} in {w_old}: a Stock Reconciliation after the cutoff counts stock {self.old} still holds, "
					f"settle this pair by hand (the formula gives {qty})")
				qty = 0
			elif qty < 0:
				notes.append(f"{item} in {w_old}: {self.old} short by {-qty} (its remaining documents used stock that moved to "
					f"{self.new}), transfer it back by hand")
			bal = q_new + max(qty, 0)
			low = bal
			for ev in new_ev:
				if ev[0]:
					break
				bal += ev[1]
				low = min(low, bal)
			if low < -0.000001 and not hold:
				notes.append(f"{item} in {w_new}: {self.new} short by {-low} (its documents used stock that stayed in {self.old}), "
					"hand it over by hand")
			if qty <= 0 and p not in self.stock_pairs and not new_ev:
				continue
			try:
				if qty > 0:
					bal = q
					for ev in old_ev:  # moved rows left OLD's later running balances stale, and posting the issue checks them
						if ev[0]:
							break
						bal += ev[1]
						frappe.db.set_value("Stock Ledger Entry", ev[2], "qty_after_transaction", bal, update_modified=False)
					out = self.stock_entry("Material Issue", self.old, posting, f"move_to_ffe stock hand-over {item} {w_old}",
						{"item_code": item, "qty": qty, "s_warehouse": w_old, "expense_account": acc[self.old], "cost_center": cc[self.old]})
					value = -flt(frappe.db.get_value("Stock Ledger Entry", {"voucher_type": "Stock Entry", "voucher_no": out.name,
						"is_cancelled": 0}, "stock_value_difference"))
					inn = self.stock_entry("Material Receipt", self.new, posting, f"move_to_ffe stock hand-over {item} {w_old} ({out.name})",
						{"item_code": item, "qty": qty, "t_warehouse": w_new, "basic_rate": value / qty, "expense_account": acc[self.new],
						"cost_center": cc[self.new]})
					notes.append(f"{item}: handed over {qty} ({value}) {out.name} -> {inn.name}")
				else:  # nothing to hand over: rebuild running balances and Bin of both warehouses
					for wh in (w_old, w_new):
						if frappe.db.exists("Stock Ledger Entry", {"item_code": item, "warehouse": wh, "is_cancelled": 0,
								"posting_date": [">=", self.cutoff]}):
							riv = frappe.get_doc({"doctype": "Repost Item Valuation", "based_on": "Item and Warehouse", "item_code": item,
								"warehouse": wh, "posting_date": posting, "posting_time": "23:59:59",
								"company": frappe.db.get_value("Warehouse", wh, "company")})
							riv.flags.ignore_permissions = True
							riv.insert()
							riv.submit()
					notes.append(f"{item}: reposting {w_old} and {w_new}")
				frappe.db.commit()
			except Exception as e:
				frappe.db.rollback()
				notes.append(f"{item} in {w_old}: hand-over failed: {e}")
		return notes

	def last_qty(self, item, wh):
		r = frappe.get_all("Stock Ledger Entry", filters={"item_code": item, "warehouse": wh, "is_cancelled": 0,
			"posting_date": ["<", self.cutoff]}, fields=["qty_after_transaction"],
			order_by="posting_date desc, posting_time desc, creation desc", limit=1)
		return r[0].qty_after_transaction if r else 0

	def stock_entry(self, purpose, company, posting, remarks, row):
		se = frappe.get_doc({"doctype": "Stock Entry", "stock_entry_type": purpose, "purpose": purpose, "company": company,
			"posting_date": posting, "posting_time": "23:59:59", "set_posting_time": 1, "remarks": remarks, "items": [row]})
		se.flags.ignore_permissions = True
		se.insert()
		se.submit()
		return se

	def fix_bins(self):
		"""Ordered / reserved / requested quantities follow the moved orders to the NEW warehouses."""
		from erpnext.stock.stock_balance import repost_stock
		from erpnext.stock.utils import get_bin

		notes = []
		pairs = set(self.bin_pairs)  # every mapped OLD Bin as well, so a re-run after an interrupted run still rebuilds them
		for item, wh in frappe.db.sql("""select b.item_code, b.warehouse from `tabBin` b join `tabWarehouse` w on w.name = b.warehouse
				where w.company = %s""", self.old):
			if wh in self.wh:
				pairs.add(item + "|" + wh)
		for p in sorted(pairs):
			item, w_old = p.split("|", 1)
			for wh in (w_old, self.wh.get(w_old)):
				if not wh:
					continue
				try:
					b = get_bin(item, wh)
					if hasattr(b, "recalculate_values"):
						b.recalculate_values()
					else:
						repost_stock(item, wh, only_bin=True)
					frappe.db.commit()
				except Exception as e:
					frappe.db.rollback()
					notes.append(f"{item} in {wh}: Bin not rebuilt: {e}")
		return notes

	# ---- reporting

	def count(self, dt):
		"""Documents of dt on OLD on/after the cutoff, for the percentage; None when counting would take over COUNT_LIMIT seconds."""
		t = time.time()
		if dt in FOLLOW:
			lf, pdt = FOLLOW[dt]
			q, v = f"""select count(*) from `tab{dt}` h join `tab{pdt}` p on p.name = h.`{lf}` where h.company = %s
				and p.company = %s""", (self.old, self.new)
		else:
			q, v = f"select count(*) from `tab{dt}` where company = %s and `{self.dts[dt]}` >= %s", (self.old, self.cutoff)
		try:
			return frappe.db.sql(f"set statement max_statement_time = {COUNT_LIMIT} for {q}", v)[0][0]
		except Exception:  # too big to count quickly: move it without a percentage
			return None
		finally:
			self.track += time.time() - t

	def progress(self, dt, last, state="running", force=False):
		"""Counts in the Error Log title, details in its text, the same in the cache for status(); every PROGRESS_EVERY seconds."""
		t = time.time()
		if state == "running" and not force and t - self.last_write < PROGRESS_EVERY:
			return
		self.last_write = t
		el = t - self.t0
		kept = sum(x["kept"] for x in self.stats.values())
		failed = sum(x["failed"] for x in self.stats.values())
		if self.dry_run:
			moved, word = sum(x["found"] for x in self.stats.values()) - kept, "would move"
		else:
			moved, word = sum(x["moved"] + x["swept"] for x in self.stats.values()), "moved"
		st = {"state": state, word: moved, "stay": kept, "failed": failed, "elapsed": fmt(el), "per_second": round(moved / el) if el else 0,
			"types_done": len(self.dts) if state != "running" else self.pos + (dt in self.finished), "types_total": len(self.dts),
			"updated": str(now_datetime())[:19]}
		lines = [f"{state}: {moved:,} {word}, {kept:,} stay, {failed:,} failed in {st['elapsed']} ({st['per_second']:,} per second)"]
		if dt and state == "running":
			x = self.stats[dt]
			done = x["moved"] - x["moved0"] + x["kept"] + x["failed"]
			rate = done / max(time.time() - x["t0"], 0.001)
			if x["total"]:
				st.update(now=dt, now_done=done, now_total=x["total"], now_percent=round(100 * done / x["total"], 1),
					now_time_left=fmt((x["total"] - done) / rate) if rate else "?")
				lines.append(f"now {dt}: {done:,} of {x['total']:,} ({st['now_percent']}%), about {st['now_time_left']} left for this type"
					+ (f", last {last}" if last else ""))
			else:
				st.update(now=dt, now_done=done)
				lines.append(f"now {dt}: {done:,} done (too big to count quickly)" + (f", last {last}" if last else ""))
		lines.append(f"document types done: {st['types_done']} of {len(self.dts)} (updated {st['updated']}); finished with documents:")
		lines += ["  " + v for v in self.finished.values()]
		st["finished_types"] = list(self.finished.values())
		title = f"move_to_ffe {'dry run ' if self.dry_run else ''}{state.split(':')[0]}: {moved:,} {word}, {failed:,} failed"
		frappe.db.set_value("Error Log", self.log.name, {"method": title[:140], "error": "\n".join(lines)}, update_modified=False)
		frappe.cache.set_value("move_to_ffe:progress", st, expires_in_sec=7 * 86400)
		print(lines[0] + (" | " + lines[1] if len(lines) > 1 and lines[1].startswith("now") else ""), flush=True)
		self.track += time.time() - t

	def say(self, text):
		print(text, flush=True)

	def summary(self, notes):
		tot = {k: sum(s[k] for s in self.stats.values()) for k in ("found", "moved", "kept", "failed", "swept")}
		lines = [("DRY RUN, would move " + str(tot["found"] - tot["kept"]) if self.dry_run else "moved " + str(tot["moved"]))
			+ f", stay {tot['kept']}, failed {tot['failed']}, re-moved {tot['swept']} ({int(time.time() - self.t0)} s)"]
		lines.append(f"progress tracking took {self.track:.1f} s of {fmt(time.time() - self.t0)}")
		for dt, s in self.stats.items():
			if not (s["found"] or s["moved"] or s["failed"]):
				continue
			lines.append(f"  {dt}: found {s['found']}, " + (f"would move {s['found'] - s['kept']}" if self.dry_run else f"moved {s['moved']}")
				+ f", stay {s['kept']}, failed {s['failed']}")
		if self.dry_run and self.keep:
			lines.append("(a dry run cannot tell what stays only because a document it settles or is made from stays)")
		if self.kept:
			lines.append(f"stay in {self.old} (documents, reason, first 20):")
			for reason, docs in sorted(self.kept.items(), key=lambda x: -len(x[1])):
				lines.append(f"  {len(docs)}  {reason}: " + ", ".join(docs[:20]))
		if self.failed:
			lines.append("failed, see Error Log 'move_to_ffe failed ...' (first 20): " + ", ".join(k.replace("|", " ") for k in list(self.failed)[:20]))
		if self.block:
			lines.append("fix these before the stock hand-over (the move itself does not wait for them), then run again:")
			lines += ["  " + b for b in self.block]
		if self.warn:
			lines.append("the stock reposts wait until:")
			lines += ["  " + w for w in self.warn]
		if self.unmapped:
			lines.append(f"{self.old} records with no {self.new} twin (documents whose ledgers use them fail; map them in extra):")
			for t, names in sorted(self.unmapped.items()):
				lines.append(f"  {t}: {len(names)}: " + ", ".join(names[:30]))
		if notes:
			lines.append("stock:")
			lines += ["  " + n for n in notes]
		if not self.dry_run:
			still = []
			for dt, s in self.stats.items():
				n = frappe.db.sql(f"select count(*) from `tab{dt}` where company = %s and `{self.dts[dt] or 'creation'}` >= %s",
					(self.old, self.cutoff))[0][0]
				if n - s["kept"] - s["failed"] > 0:
					still.append(f"  {dt}: {n - s['kept'] - s['failed']}")
			if still:
				lines.append(f"still on {self.old} although nothing keeps them (made or changed during the run): run again")
				lines += still
			if "Payment Ledger Entry" in self.ledgers:
				split = frappe.db.sql("""select distinct p.voucher_type, p.voucher_no, p.company, p.against_voucher_type, p.against_voucher_no,
						o.company from `tabPayment Ledger Entry` p join `tabPayment Ledger Entry` o on o.voucher_type = p.against_voucher_type
						and o.voucher_no = p.against_voucher_no and o.against_voucher_no = o.voucher_no and o.delinked = 0
					where p.delinked = 0 and p.posting_date >= %(cutoff)s and p.company in %(cos)s and o.company in %(cos)s
						and p.company != o.company limit 50""", {"cutoff": self.cutoff, "cos": (self.old, self.new)})
				if split:
					lines.append(f"payments in a different company than the document they settle (first 50, settle them by hand): {len(split)}")
					lines += [f"  {x[0]} {x[1]} ({x[2]}) -> {x[3]} {x[4]} ({x[5]})" for x in split]
		left = []
		for d in frappe.get_all("DocType", filters={"is_submittable": 1, "istable": 0}, pluck="name"):
			if d in self.dts or d in LEDGERS or d in ("Repost Item Valuation", "Serial and Batch Bundle") or not frappe.db.table_exists(d):
				continue
			cols = self.cols(d)
			f = next((g for g in ("posting_date", "transaction_date") if g in cols), None)
			if "company" in cols and f:
				c = frappe.db.sql(f"select count(*) from `tab{d}` where company = %s and `{f}` >= %s and docstatus < 2", (self.old, self.cutoff))[0][0]
				if c:
					left.append(f"  {d}: {c}" + (" (redo the year-end closing after the move)" if d == "Period Closing Voucher" else ""))
		if left:
			lines.append(f"other {self.old} documents on/after {self.cutoff} this move leaves alone:")
			lines += left
		return lines

	def children(self, dt):
		return list(dict.fromkeys(tf.options for tf in frappe.get_meta(dt).get_table_fields()))

	def cols(self, t):
		if t not in self.columns:
			self.columns[t] = set(frappe.db.get_table_columns(t))
		return self.columns[t]
