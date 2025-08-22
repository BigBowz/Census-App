import streamlit as st
import pandas as pd
from io import BytesIO
from datetime import datetime
import re
from openpyxl import load_workbook

from amplify_census_input import normalize_and_validate
from export_aetna_afa import export_to_aetna_afa

st.set_page_config(page_title="Census Converter", layout="wide")
st.title("Census Converter")
st.caption("Upload → Pick Sheet & Rows → Normalize & Validate → Convert → Download")

# ---------- helpers to read cells & build filename ----------
def read_sheet_fields(file_bytes: bytes, sheet_name: str) -> dict:
    """
    Read company/address/fein/sic from the original uploaded sheet:
      C5 -> company
      C6 -> address
      C8 -> fein
      E7 -> sic
    """
    wb = load_workbook(filename=BytesIO(file_bytes), read_only=True, data_only=True)
    try:
        ws = wb[sheet_name]
        company = ws["C5"].value
        address = ws["C6"].value
        fein    = ws["C8"].value
        sic     = ws["E7"].value
    finally:
        wb.close()

    def _clean(v):
        s = "" if v is None else str(v).strip()
        return re.sub(r"\s+", " ", s)

    return {
        "company": _clean(company),
        "address": _clean(address),
        "fein":    _clean(fein),
        "sic":     _clean(sic),
    }

def sanitize_filename_component(s: str) -> str:
    s = re.sub(r'[\\/:*?"<>|]+', "", s or "")
    s = re.sub(r"\s+", " ", s).strip()
    return s or "Unknown Company"


uploaded = st.file_uploader("Upload census Excel (.xlsx)", type=["xlsx"])

if uploaded is not None:
    file_bytes = uploaded.getvalue()

    # 1) Sheet picker
    try:
        xls = pd.ExcelFile(BytesIO(file_bytes))
        sheets = xls.sheet_names
    except Exception as e:
        st.error(f"Could not open workbook: {e}")
        st.stop()

    with st.expander("Step 1 — Choose sheet & rows", expanded=True):
        colA, colB, colC = st.columns([1.2, 1, 1])

        with colA:
            sheet = st.selectbox("Sheet", options=sheets, index=0)

        # Raw preview to help pick header/data rows (no header)
        try:
            raw_preview = pd.read_excel(BytesIO(file_bytes), sheet_name=sheet, header=None, nrows=30)
        except Exception as e:
            st.error(f"Could not read preview: {e}")
            st.stop()

        raw_preview_display = raw_preview.copy()
        raw_preview_display.index = raw_preview_display.index + 1  # Excel-like row numbers
        st.write("**Raw preview (first 30 rows, no headers yet)**")
        st.dataframe(raw_preview_display, use_container_width=True)

        max_row = max(30, len(raw_preview_display))
        with colB:
            header_row_excel = st.number_input(
                "Header row (Excel #)",
                min_value=1, max_value=max_row, value=1, step=1,
                help="Row that contains the column names",
            )
        with colC:
            first_data_row_excel = st.number_input(
                "First data row (Excel #)",
                min_value=header_row_excel + 1, max_value=max_row + 10,
                value=header_row_excel + 1, step=1,
                help="Usually header + 1",
            )

        # Parse with selected rows
        try:
            parsed_df = pd.read_excel(
                BytesIO(file_bytes), sheet_name=sheet, header=int(header_row_excel - 1)
            )
            # If user says data starts later than header+1, drop those extra rows
            start_offset = int(first_data_row_excel - (header_row_excel + 1))
            if start_offset > 0:
                parsed_df = parsed_df.iloc[start_offset:].reset_index(drop=True)
        except Exception as e:
            st.error(f"Could not parse with selected rows: {e}")
            st.stop()

        st.write("**Parsed preview with your header/data selection (first 20 rows)**")
        st.dataframe(parsed_df.head(20), use_container_width=True)

    # Read metadata from the same selected sheet for header cells + filename
    meta = read_sheet_fields(file_bytes, sheet)
    company_for_name = sanitize_filename_component(meta.get("company", ""))
    date_str = datetime.now().date().isoformat()
    out_filename = f"Amplify AFA Census for {company_for_name} {date_str}.xlsx"

    # 2) Normalize & validate per business rules
    try:
        norm_df, issues_df = normalize_and_validate(parsed_df)
    except Exception as e:
        st.error(f"Normalization error: {e}")
        st.stop()

    st.subheader("Normalized input (first 20 rows)")
    st.dataframe(norm_df.head(20), use_container_width=True)

    if not issues_df.empty:
        st.warning(f"Validation found {len(issues_df)} issue(s). You can still convert, but review below:")
        st.dataframe(issues_df, use_container_width=True)
        issues_csv = issues_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Download issues (CSV)",
            data=issues_csv,
            file_name="census_issues.csv",
            mime="text/csv",
        )
    else:
        st.success("No validation issues found.")

    # 3) Convert → Aetna AFA (pass meta for header cells A1/D1..A4/D4 and totals in A5)
    with st.spinner("Converting…"):
        out_bytes = export_to_aetna_afa(norm_df, meta=meta)

    # 4) Output preview
    # NOTE: headers are on Excel row 7 now, so header=6 (0-based)
    try:
        out_preview = pd.read_excel(BytesIO(out_bytes), sheet_name="Census Input", header=6)
        st.subheader("Converted output preview (first 15 rows)")
        st.dataframe(out_preview.head(15), use_container_width=True)
    except Exception as pr_err:
        st.warning(f"Output preview warning: {pr_err}")

    st.download_button(
        label="Download converted file",
        data=out_bytes,
        file_name=out_filename,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

st.markdown("---")
st.caption(
    "Filename uses C5 + today’s date. Output header cells: A1/D1 Company, A2/D2 Address, "
    "A3/D3 FEIN, A4/D4 SIC. A5 shows totals. Table headers on row 7; data starts row 8."
)
