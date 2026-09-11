"""
brand_portal_app.py — Streamlit portal for the Brand Group report
────────────────────────────────────────────────────────────────────────────
Lets you:
  1. Add/remove brands from the selected brand list (brand_portal_selected_brands).
  2. Edit per-brand-group metadata — Start Date, Legal Name, TOT Validity,
     Off Invoice Margin (%) — stored in brand_group_meta.
  3. Pick a date range and generate + email the report right away, using
     whatever brand list / metadata is currently saved.

Run with:  streamlit run brand_portal_app.py

Requires brand_portal_schema.sql to have been run once against the DB
(creates brand_portal_selected_brands and brand_group_meta). If it hasn't,
this app will show a clear error rather than fail silently.

Reuses purchase_sales_stock_report.py directly (same DB engine, same
run_report() pipeline) so there is exactly one place that knows how to
build the workbook and send the email — this app only edits settings and
triggers that same pipeline.
────────────────────────────────────────────────────────────────────────────
"""

from datetime import date, timedelta

import pandas as pd
import streamlit as st
from sqlalchemy import text

import purchase_report_portal as report

st.set_page_config(page_title="Purchase-Sale-Stock Report", layout="wide")


# =============================================================================
# DB helpers (all against the same engine purchase_sales_stock_report.py uses)
# =============================================================================

def _table_exists(table_name: str) -> bool:
    try:
        report.safe_read_sql(f'SELECT 1 FROM "{table_name}" LIMIT 1')
        return True
    except Exception:
        return False


def load_selected_brands() -> list[str]:
    return report.get_selected_brands()


def save_selected_brands(brands: list[str]) -> None:
    """Replaces the full selection: deletes anything not in `brands`,
    inserts anything new. All in one transaction."""
    with report.engine.begin() as conn:
        conn.execute(text('DELETE FROM brand_portal_selected_brands'))
        if brands:
            conn.execute(
                text(
                    'INSERT INTO brand_portal_selected_brands ("brand_name") '
                    'VALUES (:brand_name) ON CONFLICT ("brand_name") DO NOTHING'
                ),
                [{"brand_name": b} for b in brands],
            )


def load_all_known_brands() -> list[str]:
    """All brand names available to add — union of brand_sub.sub_brand and
    the current selection, so you can add brands not yet in brand_sub too."""
    try:
        brand_sub_df = report.get_brand_sub_mapping()
        known = set(brand_sub_df["sub_brand"].dropna().tolist())
    except Exception:
        known = set()
    known |= set(report.BRANDS)
    known |= set(load_selected_brands())
    return sorted(known)


def load_all_brand_groups() -> list[str]:
    """All brand_group values currently reachable from brand_sub, so the
    metadata editor has something to show even before every group has a
    brand_group_meta row yet."""
    try:
        brand_sub_df = report.get_brand_sub_mapping()
        groups = set(brand_sub_df["brand_group"].dropna().tolist())
    except Exception:
        groups = set()
    try:
        meta_df = report.get_brand_group_meta()
        groups |= set(meta_df["brand_group"].dropna().tolist())
    except Exception:
        pass
    return sorted(groups)


def load_brand_group_meta_editor_df() -> pd.DataFrame:
    """Full editor grid: every known brand_group, left-joined with whatever
    metadata already exists, so unconfigured groups show up with blank
    editable cells rather than being absent from the editor."""
    all_groups = load_all_brand_groups()
    meta_df = report.get_brand_group_meta()

    base = pd.DataFrame({"brand_group": all_groups})
    merged = base.merge(meta_df, on="brand_group", how="left")

    if "start_date" in merged.columns:
        merged["start_date"] = pd.to_datetime(merged["start_date"], errors="coerce").dt.date
    else:
        merged["start_date"] = None

    # Text columns: a brand_group with no brand_group_meta row comes out of
    # the left join with NaN (a float) in these cells, not None — whether
    # or not the column existed before the merge. st.column_config.TextColumn
    # calls len() on cell values when rendering/validating, so a leftover
    # NaN float crashes the editor ("object of type 'float' has no len()").
    # Normalize every NaN to a real None, for both the "column already
    # existed" and "column was missing" cases.
    for col in ["legal_name", "tot_validity"]:
        if col not in merged.columns:
            merged[col] = None
        else:
            merged[col] = merged[col].astype(object).where(merged[col].notna(), None)

    if "off_invoice_margin_pct" not in merged.columns:
        merged["off_invoice_margin_pct"] = None

    merged = merged[["brand_group", "start_date", "legal_name", "tot_validity", "off_invoice_margin_pct"]]
    merged.columns = ["Brand Group", "Start Date", "Legal Name", "TOT Validity", "Off Invoice Margin (%)"]
    return merged


def save_brand_group_meta(edited_df: pd.DataFrame) -> None:
    """Upserts every row in the editor grid into brand_group_meta."""
    rows = edited_df.rename(columns={
        "Brand Group": "brand_group",
        "Start Date": "start_date",
        "Legal Name": "legal_name",
        "TOT Validity": "tot_validity",
        "Off Invoice Margin (%)": "off_invoice_margin_pct",
    }).to_dict(orient="records")

    with report.engine.begin() as conn:
        for row in rows:
            if not row.get("brand_group"):
                continue
            conn.execute(
                text("""
                    INSERT INTO brand_group_meta
                        ("brand_group", "start_date", "legal_name", "tot_validity", "off_invoice_margin_pct", "updated_at")
                    VALUES
                        (:brand_group, :start_date, :legal_name, :tot_validity, :off_invoice_margin_pct, now())
                    ON CONFLICT ("brand_group") DO UPDATE SET
                        "start_date" = EXCLUDED."start_date",
                        "legal_name" = EXCLUDED."legal_name",
                        "tot_validity" = EXCLUDED."tot_validity",
                        "off_invoice_margin_pct" = EXCLUDED."off_invoice_margin_pct",
                        "updated_at" = now()
                """),
                {
                    "brand_group": row["brand_group"],
                    "start_date": row.get("start_date") or None,
                    "legal_name": row.get("legal_name") or None,
                    "tot_validity": row.get("tot_validity") or None,
                    "off_invoice_margin_pct": row.get("off_invoice_margin_pct")
                        if pd.notna(row.get("off_invoice_margin_pct")) else None,
                },
            )


# =============================================================================
# Schema check — fail loudly and clearly rather than mysteriously
# =============================================================================

missing_tables = [
    t for t in ("brand_portal_selected_brands", "brand_group_meta") if not _table_exists(t)
]
if missing_tables:
    st.error(
        f"Missing table(s): {', '.join(missing_tables)}. "
        f"Run brand_portal_schema.sql against the database once, then reload this page."
    )
    st.stop()


# =============================================================================
# UI
# =============================================================================

st.title("Brand Group Portal")

tab_brands, tab_meta, tab_generate = st.tabs(["Brand Selection", "Brand Group Settings", "Generate & Send Report"])


# ── Tab 1: Brand Selection ───────────────────────────────────────────────
with tab_brands:
    st.subheader("Selected brands")
    st.caption(
        "Only brands selected here are included in the report (both the Product Wise "
        "and Brand Group sheets)."
    )

    all_known = load_all_known_brands()
    current_selection = load_selected_brands()

    selected = st.multiselect(
        "Brands included in the report",
        options=all_known,
        default=[b for b in current_selection if b in all_known],
    )

    extra = st.text_input(
        "Add a brand not in the list above (exact spelling as it appears in billing_data/grn_data)",
        value="",
    )

    if st.button("Save brand selection", type="primary"):
        final_list = list(dict.fromkeys(selected + ([extra.strip()] if extra.strip() else [])))
        if not final_list:
            st.warning("Selection can't be empty — nothing saved.")
        else:
            save_selected_brands(final_list)
            st.success(f"Saved {len(final_list)} selected brands.")
            st.rerun()


# ── Tab 2: Brand Group Settings ──────────────────────────────────────────
with tab_meta:
    st.subheader("Per-brand-group settings")
    st.caption(
        "Start Date, Legal Name, TOT Validity, and Off Invoice Margin (%) — blank rows "
        "show blank in the report until filled in here. Off Invoice Value = "
        "MAX(Purchase Amount, Sale Revenue) × Off Invoice Margin (%)."
    )

    editor_df = load_brand_group_meta_editor_df()

    edited_df = st.data_editor(
        editor_df,
        num_rows="fixed",
        use_container_width=True,
        column_config={
            "Brand Group": st.column_config.TextColumn(disabled=True),
            "Start Date": st.column_config.DateColumn(),
            "Legal Name": st.column_config.TextColumn(),
            "TOT Validity": st.column_config.TextColumn(),
            "Off Invoice Margin (%)": st.column_config.NumberColumn(min_value=0.0, max_value=100.0, step=0.1),
        },
        key="brand_group_meta_editor",
    )

    if st.button("Save brand group settings", type="primary"):
        save_brand_group_meta(edited_df)
        st.success("Saved.")
        st.rerun()


# ── Tab 3: Generate & Send Report ────────────────────────────────────────
with tab_generate:
    st.subheader("Generate & send the report")

    default_start, default_end_exclusive, default_end_inclusive = report.get_last_6_months_window()

    col1, col2 = st.columns(2)
    with col1:
        picked_start = st.date_input("Start date", value=default_start)
    with col2:
        picked_end = st.date_input("End date (inclusive)", value=default_end_inclusive)

    if picked_end < picked_start:
        st.error("End date must be on or after the start date.")
    else:
        st.caption(
            f"This will run the report for **{picked_start:%d %b %Y} to {picked_end:%d %b %Y}** "
            f"using the brand list and brand-group settings saved in the other two tabs, "
            f"then email it via the same delivery logic as the scheduled report."
        )

        if st.button("Generate & Send Report", type="primary"):
            end_exclusive = picked_end + timedelta(days=1)
            with st.spinner("Generating report and sending email — this can take a minute..."):
                try:
                    result = report.run_report(picked_start, end_exclusive, picked_end)
                    rows = result["rows"]
                    st.success(
                        f"Done. **{result['filename']}** ({result['size_mb']} MB) generated and "
                        f"emailed via {result['delivery'].replace('_', ' ')} — "
                        f"Product Wise: {rows['product_wise']} rows, "
                        f"Brand Group: {rows['brand_group']} rows, "
                        f"Mondelez Product Wise: {rows['mondelez_product_wise']} rows. "
                        f"The workbook was not saved to disk."
                    )
                except Exception as e:
                    st.error(f"Report generation/delivery failed: {e}")