# export_aetna_afa.py
from __future__ import annotations
from io import BytesIO
from typing import Union, Optional, Dict, Any
import pandas as pd
import numpy as np

SHEET_NAME_OUT = "Census Input"
HEADERS_OUT = [
    "Last Name",
    "First Name",
    "Home Zip Code",
    "Date of Birth",
    "Gender",
    "Medical Tier",
    "Dental Tier",
    "Member Type",
    "Subscriber Employment Status",
    "",  # blank J per spec
]

AETNA_TIER_MAP = {
    "EE": "EEOnly",
    "ES": "EESpOnly",
    "EC": "EEChren",
    "FAM": "EEFamily",
    "WO": "Waive",
}

def _to_bytes_with_layout(
    df_out: pd.DataFrame,
    meta: Optional[Dict[str, Any]] = None,
    totals: Optional[Dict[str, int]] = None,
) -> bytes:
    """
    Layout:
      - Table headers on **row 7** (startrow=6), so data begins on **row 8**
      - A1: Company Name:      D1: {company}
      - A2: Address:           D2: {address}
      - A3: FEIN:              D3: {fein}
      - A4: SIC Code:          D4: {sic}
      - A5: "Total Employees: X | Total Enrolled: Y"
      - Row 6 intentionally left blank as a spacer
    """
    meta = meta or {}
    totals = totals or {}
    company = str(meta.get("company", "") or "")
    address = str(meta.get("address", "") or "")
    fein    = str(meta.get("fein", "") or "")
    sic     = str(meta.get("sic", "") or "")
    total_employees = int(totals.get("total_employees", 0))
    total_enrolled  = int(totals.get("total_enrolled", 0))

    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        # startrow=6 -> headers at Excel row 7, data at row 8
        df_out.to_excel(writer, index=False, sheet_name=SHEET_NAME_OUT, startrow=6)
        ws = writer.sheets[SHEET_NAME_OUT]

        # Top-left info block
        ws["A1"] = "Company Name:"
        ws["D1"] = company

        ws["A2"] = "Address:"
        ws["D2"] = address

        ws["A3"] = "FEIN:"
        ws["D3"] = fein

        ws["A4"] = "SIC Code:"
        ws["D4"] = sic

        # Totals line
        ws["A5"] = f"Total Employees: {total_employees} | Total Enrolled: {total_enrolled}"

    return bio.getvalue()

def export_to_aetna_afa(
    input_df: pd.DataFrame,
    meta: Optional[Dict[str, Any]] = None,
    output_path: Optional[str] = None
) -> Union[bytes, str]:
    """
    Expects a **normalized** DataFrame (from amplify_census_input).
    - Dental Tier is BLANK for all rows.
    - Headers on row 7, data on row 8.
    - A1/D1..A4/D4 from 'meta'; A5 shows totals.
    """
    # Build MEDICAL tier (subscriber defines; dependents inherit last subscriber)
    med = []
    last_sub_tier = ""
    for _, r in input_df.iterrows():
        if r["Member Type"] == "EE":
            t = AETNA_TIER_MAP.get(r["Coverage Tier"], "")
            last_sub_tier = t
            med.append(t)
        else:
            med.append(last_sub_tier)

    # Totals
    total_employees = int((input_df["Member Type"] == "EE").sum())
    waived_ee = int(((input_df["Member Type"] == "EE") & (pd.Series(med) == "Waive")).sum())
    total_enrolled = max(total_employees - waived_ee, 0)

    # Assemble output table
    out = pd.DataFrame()
    out["Last Name"] = input_df["Last Name"]
    out["First Name"] = input_df["First Name"]
    out["Home Zip Code"] = input_df["Home Zip Code"]
    out["Date of Birth"] = input_df["Date of Birth"]
    out["Gender"] = input_df["Gender"]
    out["Medical Tier"] = med
    out["Dental Tier"] = ""  # always blank
    out["Member Type"] = input_df["Member Type"]
    out["Subscriber Employment Status"] = np.where(
        input_df["Member Type"] == "EE", input_df["Work Status"], ""
    )
    out[""] = ""

    out = out[HEADERS_OUT]

    xbytes = _to_bytes_with_layout(
        out,
        meta=meta,
        totals={"total_employees": total_employees, "total_enrolled": total_enrolled},
    )

    if output_path:
        with open(output_path, "wb") as f:
            f.write(xbytes)
        return output_path
    return xbytes
