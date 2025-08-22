# amplify_census_input.py
# Ingestion + normalization + validation for Amplify census files.

from __future__ import annotations
from io import BytesIO
from typing import Dict, List, Tuple, Union
import pandas as pd
import re
import unicodedata

# ----------------------------
# Standardized (normalized) columns
# ----------------------------
STANDARD_COLS = [
    "First Name",
    "Last Name",
    "Gender",                 # M / F / ""
    "Date of Birth",          # MM/DD/YYYY
    "Home Zip Code",          # 5-digit ZIP (leading zeros preserved)
    "Member Type",            # EE / SP / CH
    "Coverage Tier",          # EE / EC / ES / FAM / WO
    "Work Status",            # Active / Cobra / ""
    "Work Zip",               # optional 5-digit ZIP
    "Current Medical Plan",   # optional passthrough
]

# ----------------------------
# Flexible header aliases (case-insensitive, after header normalization)
# ----------------------------
ALIASES: Dict[str, str] = {
    # Names
    "first name": "First Name",
    "firstname": "First Name",
    "fname": "First Name",
    "last name": "Last Name",
    "lastname": "Last Name",
    "lname": "Last Name",
    "surname": "Last Name",
    "full name": "Full Name",
    "employee name": "Full Name",
    "name": "Full Name",

    # Demographics
    "gender": "Gender",
    "sex": "Gender",
    "dob": "Date of Birth",
    "date of birth": "Date of Birth",
    "birth date": "Date of Birth",

    # Address / ZIP
    "home zip code": "Home Zip Code",
    "home zip": "Home Zip Code",
    "zip": "Home Zip Code",
    "zip code": "Home Zip Code",
    "zipcode": "Home Zip Code",
    "postal code": "Home Zip Code",
    "work zip": "Work Zip",
    "work zip code": "Work Zip",

    # Relationship / member
    "member type": "Member Type",
    "member": "Member Type",
    "relationship": "Member Type",
    "subscriber/dependent": "Member Type",

    # Coverage
    "coverage tier": "Coverage Tier",
    "coverage": "Coverage Tier",
    "medical tier": "Coverage Tier",

    # Status
    "work status": "Work Status",
    "employment status": "Work Status",
    "subscriber employment status": "Work Status",

    # Plan (optional)
    "current medical plan": "Current Medical Plan",
    "medical plan": "Current Medical Plan",
}

# ----------------------------
# Header normalization
# ----------------------------
def _normalize_header_label(s: str) -> str:
    """
    Clean raw header text so aliases match even if the source has line breaks,
    hints in parentheses, trailing colons, or odd spaces.
    Example: "Gender\n(M or F)" -> "gender"
    """
    if s is None:
        return ""
    s = str(s).strip()
    s = unicodedata.normalize("NFKC", s)
    s = re.sub(r"[\r\n\t]+", " ", s)         # remove line breaks/tabs
    s = re.sub(r"\(.*?\)", "", s)            # drop (...) hints
    s = re.sub(r"\[.*?\]", "", s)            # drop [...] hints
    s = re.sub(r"\s{2,}", " ", s)            # collapse spaces
    s = re.sub(r"\s*:\s*$", "", s)           # drop trailing colon
    return s.strip().lower()

# ----------------------------
# Name splitting (handles "Last, First Middle Suffix" and "First Middle Last Suffix")
# ----------------------------
_SUFFIXES = {"JR", "SR", "II", "III", "IV", "V"}

def _split_full_name(name: str) -> tuple[str, str, list[str]]:
    """
    Return (first, last, warnings).
    - Comma style: 'Last, First Middle Suffix'
    - Space style: 'First Middle Last Suffix'
    - If one token only, treat as Last Name (warn).
    """
    warnings: list[str] = []
    s = (name or "").strip()
    if not s:
        return "", "", warnings

    # "Last, First Middle Suffix"
    if "," in s:
        last, rest = [p.strip() for p in s.split(",", 1)]
        tail = rest.split()
        if not tail:
            return "", last, ["Full Name split yielded empty first name"]
        first = tail[0]
        suffix = [t for t in tail[1:] if t.upper().strip(".") in _SUFFIXES]
        middle = [t for t in tail[1:] if t not in suffix]
        last_full = " ".join([last] + middle + suffix) if (middle or suffix) else last
        return first, last_full, warnings

    # "First Middle Last Suffix"
    toks = s.split()
    if len(toks) == 1:
        return "", toks[0], ["Full Name has single token; treated as last name"]
    first = toks[0]
    tail = toks[1:]
    suffix = [t for t in tail if t.upper().strip(".") in _SUFFIXES]
    core = [t for t in tail if t not in suffix]
    if not core:
        return first, " ".join(suffix), ["Name missing core last name; suffix only"]
    last = " ".join(core + suffix)
    return first, last, warnings

# ----------------------------
# Field normalizers
# ----------------------------
def _norm_zip(val) -> str:
    if pd.isna(val):
        return ""
    s = str(val).strip()
    if "-" in s:
        s = s.split("-")[0]            # keep ZIP5 when ZIP+4 provided
    s = re.sub(r"\D", "", s)           # digits only
    return s.zfill(5)[:5] if s else "" # 5-digit with leading zeros

def _norm_dob(val) -> str:
    # Normalize any Excel/ISO/text date to MM/DD/YYYY (or blank if invalid)
    if pd.isna(val) or str(val).strip() == "":
        return ""
    dt = pd.to_datetime(val, errors="coerce")
    return "" if pd.isna(dt) else dt.strftime("%m/%d/%Y")

def _norm_gender(val) -> str:
    if pd.isna(val) or str(val).strip() == "":
        return ""
    s = str(val).strip().upper()
    if s in {"M", "MALE"}: return "M"
    if s in {"F", "FEMALE"}: return "F"
    return ""  # allow blank for unknown/other

# Member Type mapping (unknown job titles -> EE)
_MEMBER_ALIASES = {
    "EE": "EE", "E": "EE", "SUB": "EE", "SUBSCRIBER": "EE", "EMPLOYEE": "EE",
    "SP": "SP", "S": "SP", "SPOUSE": "SP", "DOMESTIC PARTNER": "SP", "DP": "SP", "PARTNER": "SP",
    "C": "CH", "CH": "CH", "CHILD": "CH", "CHILDREN": "CH", "KID": "CH", "DEPENDENT": "CH",
}
def _norm_member(val) -> str:
    if pd.isna(val) or str(val).strip() == "":
        return ""
    s = str(val).strip().upper()
    if s in _MEMBER_ALIASES:
        return _MEMBER_ALIASES[s]
    # Treat unknown role labels (often job titles) as Employee
    return "EE"

# Coverage Tier mapping
_COV_ALIASES = {
    # Employee Only
    "E": "EE", "EO": "EE", "EMPLOYEE": "EE", "EMPLOYEE ONLY": "EE", "EEONLY": "EE", "EE ONLY": "EE", "EE": "EE",
    # Employee + Spouse
    "ES": "ES", "EMPLOYEE + SPOUSE": "ES", "EE + SPOUSE": "ES", "EESPONLY": "ES", "EES PONLY": "ES", "EESPONLY": "ES",
    # Employee + Child(ren)
    "EC": "EC", "EMPLOYEE + CHILD": "EC", "EMPLOYEE + CHILDREN": "EC", "EE + CHILD": "EC", "EE + CHILDREN": "EC", "EECHREN": "EC",
    # Family
    "F": "FAM", "FAM": "FAM", "FAMILY": "FAM", "EE + FAMILY": "FAM", "EE&FAMILY": "FAM",
    # Waive
    "WO": "WO", "WAIVE": "WO", "WAIVED": "WO", "WAV": "WO", "NONE": "WO", "NO COVERAGE": "WO",
}
def _norm_coverage(val) -> str:
    if pd.isna(val) or str(val).strip() == "":
        return ""
    s = str(val).strip().upper()
    s = re.sub(r"\s*\+\s*", " + ", s)  # normalize '+' spacing
    s = s.replace("&", "+")            # allow & as '+'
    return _COV_ALIASES.get(s, s if s in {"EE", "EC", "ES", "FAM", "WO"} else "")

# Work Status: only Active/Cobra kept; others blank + warning message
_ALLOWED_STATUS = {"ACTIVE": "Active", "COBRA": "Cobra"}
def _norm_status(val) -> tuple[str, str]:
    if pd.isna(val) or str(val).strip() == "":
        return "", ""
    s = str(val).strip().upper()
    if s in _ALLOWED_STATUS:
        return _ALLOWED_STATUS[s], ""
    return "", f"Ignored non-supported Work Status '{val}' (only Active/Cobra kept)"

# ----------------------------
# Column picking using normalized header labels
# ----------------------------
def _pick_cols(df: pd.DataFrame) -> Dict[str, str]:
    """
    Map incoming columns to standardized names using cleaned header labels.
    Returns a dict of source_col_name -> STANDARD name.
    """
    inv = {k.lower(): v for k, v in ALIASES.items()}
    mapping: Dict[str, str] = {}
    for c in df.columns:
        key = _normalize_header_label(c)
        if key in inv:
            mapping[c] = inv[key]
        else:
            # Heuristics for common patterns after cleaning
            if key.startswith("gender"):
                mapping[c] = "Gender"
            elif key in {"dob", "date of birth", "birth date"}:
                mapping[c] = "Date of Birth"
            elif key.startswith("home zip"):
                mapping[c] = "Home Zip Code"
            elif key.startswith("work zip"):
                mapping[c] = "Work Zip"
            elif key.startswith("coverage"):
                mapping[c] = "Coverage Tier"
            elif key.startswith("employment status") or key.startswith("work status"):
                mapping[c] = "Work Status"
            elif key in {"fullname", "full name", "employee name", "name"}:
                mapping[c] = "Full Name"
            # else: leave unmapped (ignored unless added to ALIASES)
    return mapping

def _ensure_all_cols(df: pd.DataFrame) -> pd.DataFrame:
    for col in STANDARD_COLS:
        if col not in df.columns:
            df[col] = ""
    return df[STANDARD_COLS]

# ----------------------------
# Public API
# ----------------------------
def load_any(input_data: Union[bytes, str, pd.DataFrame]) -> pd.DataFrame:
    """Read first sheet to a DataFrame from bytes / path / DF."""
    if isinstance(input_data, pd.DataFrame):
        return input_data.copy()
    if isinstance(input_data, (bytes, bytearray)):
        return pd.read_excel(BytesIO(input_data))
    if isinstance(input_data, str):
        return pd.read_excel(input_data)
    raise ValueError("Unsupported input type.")

def normalize_and_validate(input_data: Union[bytes, str, pd.DataFrame]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns:
      normalized_df: DataFrame with STANDARD_COLS
      issues_df:     DataFrame with Row, First Name, Last Name, Issues
    """
    raw = load_any(input_data)

    # Map known headers to standard names
    mapping = _pick_cols(raw)
    df = pd.DataFrame()
    for src_col, std_col in mapping.items():
        df[std_col] = raw[src_col]

    # Handle Full Name -> split into First/Last if needed
    split_warnings: List[Dict[str, str]] = []
    if "Full Name" in df.columns:
        if "First Name" not in df.columns and "Last Name" not in df.columns:
            firsts, lasts = [], []
            for idx, val in df["Full Name"].items():
                f, l, warns = _split_full_name("" if pd.isna(val) else str(val))
                firsts.append(f)
                lasts.append(l)
                for w in warns:
                    split_warnings.append({"Row": idx + 2, "First Name": f, "Last Name": l, "Issues": w})
            df["First Name"] = firsts
            df["Last Name"]  = lasts
        # Drop Full Name once split/used
        df = df.drop(columns=["Full Name"])

    # Ensure all standard columns exist
    df = _ensure_all_cols(df)

    # Normalize fields
    df["First Name"]      = df["First Name"].fillna("").astype(str).str.strip()
    df["Last Name"]       = df["Last Name"].fillna("").astype(str).str.strip()
    df["Gender"]          = df["Gender"].apply(_norm_gender)
    df["Date of Birth"]   = df["Date of Birth"].apply(_norm_dob)
    df["Home Zip Code"]   = df["Home Zip Code"].apply(_norm_zip)
    df["Work Zip"]        = df["Work Zip"].apply(_norm_zip)
    df["Member Type"]     = df["Member Type"].apply(_norm_member)
    df["Coverage Tier"]   = df["Coverage Tier"].apply(_norm_coverage)
    df["Current Medical Plan"] = df["Current Medical Plan"].fillna("").astype(str).str.strip()

    # Normalize Work Status and collect warnings
    status_vals: List[str] = []
    status_issues: List[Dict[str, str]] = []
    for idx, val in df["Work Status"].items():
        norm, msg = _norm_status(val)
        status_vals.append(norm)
        if msg:
            status_issues.append({
                "Row": idx + 2,
                "First Name": df.at[idx, "First Name"],
                "Last Name": df.at[idx, "Last Name"],
                "Issues": msg
            })
    df["Work Status"] = status_vals

    # Build validation issues
    issues: List[Dict[str, str]] = []
    issues.extend(split_warnings)

    for i, row in df.iterrows():
        row_issues: List[str] = []

        # Required for all: First/Last name
        if not row["First Name"]:
            row_issues.append("Missing First Name")
        if not row["Last Name"]:
            row_issues.append("Missing Last Name")

        # Member Type must be EE/SP/CH
        if row["Member Type"] not in {"EE", "SP", "CH"}:
            row_issues.append("Member Type must be EE/SP/CH")

        # Coverage Tier must be allowed (or blank)
        if row["Coverage Tier"] not in {"EE", "EC", "ES", "FAM", "WO", ""}:
            row_issues.append("Coverage Tier invalid")

        # Subscriber (EE) must have DOB + Home Zip
        if row["Member Type"] == "EE":
            if not row["Date of Birth"]:
                row_issues.append("Subscriber missing DOB")
            if not row["Home Zip Code"]:
                row_issues.append("Subscriber missing Home Zip")

        # Warn if Work Status present on non-EE
        if row["Member Type"] != "EE" and row["Work Status"]:
            row_issues.append("Work Status should be blank for non-EE")

        if row_issues:
            issues.append({
                "Row": i + 2,  # Excel-like row number (header is row 1)
                "First Name": row["First Name"],
                "Last Name": row["Last Name"],
                "Issues": "; ".join(row_issues)
            })

    # Add Work Status ignore-warnings
    issues.extend(status_issues)

    issues_df = pd.DataFrame(
        issues, columns=["Row", "First Name", "Last Name", "Issues"]
    ) if issues else pd.DataFrame(columns=["Row", "First Name", "Last Name", "Issues"])

    return df, issues_df
