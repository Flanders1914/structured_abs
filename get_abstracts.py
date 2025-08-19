#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
get_abstracts.py (xtract-based)

Read local PubMed UIDs (one PMID per line), fetch PubMed XML in batches via
EDirect (epost | efetch -format xml), parse fields with EDirect's `xtract`,
and write one JSON object per line (JSONL).

We deliberately rely on xtract (official EDirect parser) instead of Python's
ElementTree to extract fields.

Emitted fields:
- pmid
- title
- journal
- journal_iso_abbr
- volume
- issue
- pub_date: {"year": "...", "month": "...", "day": "..."}
- abstract: [{"label": "...", "nlm_category": "...", "text": "..."}, ...]

References:
- epost/efetch/xtract pipeline (NLM/NCBI) – official EDirect docs.
- xtract: rows/columns, exploration with -block, variables (-PMID / &PMID).
- PubMed DTD: AbstractText has Label/NlmCategory attributes (structured abstracts).
"""

import os
import json
import time
import subprocess
from typing import List, Dict, Optional

from tqdm import tqdm
from config import BATCH_SIZE, NCBI_API_KEY, DATA_DIR, BEGIN_YEAR, END_YEAR, FILE_SUFFIX

# Progress bar with ETA (tqdm); fallback to simple prints if not available.

MONTH_MAP = {
    "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04",
    "May": "05", "Jun": "06", "Jul": "07", "Aug": "08",
    "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12",
}

# ----------------------- Small helpers -----------------------
def _count_lines(path: str) -> int:
    """Count lines (each should contain one PMID)."""
    n = 0
    with open(path, "r", encoding="utf-8") as f:
        for _ in f:
            n += 1
    return n

def _run_pipeline(cmd: List[str], input_bytes: Optional[bytes] = None) -> bytes:
    """Run a command (list-form), optionally feeding stdin bytes; return stdout bytes, raise on error."""
    proc = subprocess.Popen(cmd, stdin=subprocess.PIPE if input_bytes else None,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = proc.communicate(input=input_bytes)
    if proc.returncode != 0:
        raise RuntimeError(f"Command failed ({proc.returncode}): {' '.join(cmd)}\n{err.decode('utf-8', 'ignore')}")
    return out

# ----------------------- Core xtract steps -----------------------

def _epost_efetch_to_xml(uids: List[str], xml_path: str) -> None:
    """
    Send UIDs to epost, then efetch XML into a temp file.
    Uses one network call per batch. We reuse the same XML file for multiple xtract passes.
    """
    # Prepare the newline-delimited UID payload once
    data = ("\n".join(uids) + "\n").encode("utf-8")

    # Use bash with pipefail so failures in either stage propagate as non-zero.
    # Also pass -db pubmed to efetch explicitly for readability.
    with open(xml_path, "wb") as fout:
        proc = subprocess.Popen(
            ["bash", "-lc", "set -o pipefail; epost -db pubmed | efetch -db pubmed -format xml"],
            stdin=subprocess.PIPE, stdout=fout, stderr=subprocess.PIPE
        )
        _ ,err = proc.communicate(input=data)  # <-- write & close stdin here
        if proc.returncode != 0:
            raise RuntimeError(
                f"epost|efetch failed (exit {proc.returncode}): {err.decode('utf-8', 'ignore')}"
            )

def _xtract_headers(xml_path: str) -> List[Dict]:
    """
    Use xtract to pull one row per article with stable columns:
    PMID, Title, Journal Title, Journal ISO Abbreviation, Volume, Issue, Year, Month, Day
    """
    cmd = [
        "xtract", "-input", xml_path,
        "-pattern", "PubmedArticle",
        "-tab", "\t", "-def", "NA",

        # ---- initialize variables (defaults) ----
        "-PMID",  "NA",
        "-TITLE", "NA",
        "-JT",    "NA",
        "-ISO",   "NA",
        "-VOL", "NA",
        "-ISS", "NA",
        "-Y",   "NA",
        "-M",   "NA",
        "-D",   "NA",

        # ---- override within blocks if they exist ----

        "-PMID",  "MedlineCitation/PMID",
        "-TITLE", "ArticleTitle",
        "-JT",    "Journal/Title",
        "-ISO",   "Journal/ISOAbbreviation",

        "-block", "Journal/JournalIssue",
        "-VOL",   "Volume",
        "-ISS",   "Issue",

        "-block", "Journal/JournalIssue/PubDate",
        "-Y",     "Year",
        "-M",     "Month",
        "-D",     "Day",

        # ---- emit in the desired column order ----
        "-element", "&PMID", "&TITLE", "&JT", "&ISO", "&VOL", "&ISS", "&Y", "&M", "&D",
    ]
    out = _run_pipeline(cmd).decode("utf-8", "replace")
    rows = []
    for line in out.splitlines():
        cols = line.rstrip("\n").split("\t")
        if len(cols) != 9:
            print(line)
            print(cols)
            print(len(cols))
            # Keep robust: skip malformed lines instead of crashing the whole batch
            continue
        pmid, title, jtitle, iso_abbr, vol, iss, yr, mon, day = cols
        # Normalize potential newlines/tabs in free text
        title = title.replace("\t", " ").replace("\r", " ").replace("\n", " ").strip()
        jtitle = jtitle.replace("\t", " ").replace("\r", " ").replace("\n", " ").strip()
        pub = {
            "pmid": pmid.strip(),
            "title": title,
            "journal": jtitle,
            "journal_iso_abbr": iso_abbr,
            "volume": vol.strip(),
            "issue": iss.strip(),
            "pub_date": {
                "year": yr.strip() or None,
                "month": (mon.strip() or None),
                "day": (day.strip() or None),
            },
        }
        rows.append(pub)
    return rows

def _xtract_abstracts(xml_path: str) -> Dict[str, List[Dict]]:
    """
    Use xtract to extract structured abstract pieces.
    """
    cmd = [
        "xtract", "-input", xml_path,
        "-pattern", "PubmedArticle",
        "-tab", "\t", "-def", "NA",
        # print PMID ONCE per row (outside the block)
        "-element", "MedlineCitation/PMID",
        # initialize other variables
        "-LABEL", "NA",
        "-NLMC", "NA",
        "-TEXT", "NA",
        # iterate each AbstractText block and print the triple for each piece
        # Override within blocks if they exist
        "-block", "MedlineCitation/Article/Abstract/AbstractText",
        "-LABEL", "AbstractText@Label",
        "-NLMC", "AbstractText@NlmCategory",
        "-TEXT", "AbstractText",
        "-element", "&LABEL", "&NLMC", "&TEXT",
    ]
    out = _run_pipeline(cmd).decode("utf-8", "replace")
    result: Dict[str, List[Dict]] = {}
    for line in out.splitlines():
        cols = line.rstrip("\n").split("\t")
        if not cols:
            continue
        pmid = cols[0].strip()
        if not pmid:
            continue
        # Each AbstractText contributes 3 columns (label, nlmcat, text) appended after the first pmid column.
        triples = cols[1:]
        if not triples:
            # no abstract
            continue
        if len(triples) % 3 != 0:
            print(f"We have {len(triples)} triples")
            print(triples)
            continue
        pieces = []
        for i in range(0, len(triples), 3):
            label = triples[i] if i < len(triples) else ""
            nlm = triples[i+1] if i+1 < len(triples) else ""
            text = triples[i+2] if i+2 < len(triples) else ""
            # Clean tabs/newlines just in case
            pieces.append({
                "label": (label.strip() or None),
                "nlm_category": (nlm.strip() or None),
                "text": text.replace("\t", " ").replace("\r", " ").replace("\n", " ").strip()
            })
        if pieces:
            result.setdefault(pmid, []).extend(pieces)
    return result

# ----------------------- Runner -----------------------

def run(uid_path: str, out_path: str, batch: int = 8000, api_key: Optional[str] = None):
    """
    Read UIDs, fetch XML by batch via EDirect, parse with xtract, and write JSONL.
    `batch` should be <= 10000 (History server page size).
    """
    if api_key and not os.environ.get("NCBI_API_KEY"):
        os.environ["NCBI_API_KEY"] = api_key

    total_uids = _count_lines(uid_path)
    print(f"We have {total_uids} uids to process")
    print('_'*100)
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    tmp_out = out_path + ".tmp"

    # Progress bar setup (ETA included by tqdm)
    pbar = tqdm(total=total_uids, unit="rec", desc="Fetching & parsing with xtract") if tqdm else None
    started = time.time()

    processed = 0
    written = 0

    with open(uid_path, "r", encoding="utf-8") as fin, open(tmp_out, "w", encoding="utf-8") as fout:
        chunk: List[str] = []
        for line in fin:
            pmid = line.strip()
            if not pmid:
                continue
            chunk.append(pmid)

            if len(chunk) >= batch:
                written += _process_batch(chunk, fout)
                processed += len(chunk)
                if pbar:
                    pbar.update(len(chunk))
                else:
                    if processed % batch == 0:
                        rate = processed / max(time.time() - started, 1e-6)
                        remain = total_uids - processed
                        eta = remain / max(rate, 1e-6)
                        print(f"[{processed}/{total_uids}] rate={rate:.1f} rec/s, ETA={eta/60:.1f}m")
                chunk.clear()

        if chunk:
            written += _process_batch(chunk, fout)
            processed += len(chunk)
            if pbar:
                pbar.update(len(chunk))
            else:
                rate = processed / max(time.time() - started, 1e-6)
                remain = total_uids - processed
                eta = remain / max(rate, 1e-6)
                print(f"[{processed}/{total_uids}] rate={rate:.1f} rec/s, ETA={eta/60:.1f}m")

    if pbar:
        pbar.close()

    os.replace(tmp_out, out_path)
    print(f"Processed {processed} PMIDs, wrote {written} JSONL records to {out_path}")

def _process_batch(uids: List[str], fout) -> int:
    """Fetch one batch to an XML file, run two xtract passes, then emit JSONL."""
    batch_xml_path = "temp/batch.xml"
    # create the temp directory if not exists
    os.makedirs("temp", exist_ok=True)
    # if batch_xml_path exists, clear it, else create it
    if os.path.exists(batch_xml_path):
        os.remove(batch_xml_path)

    _epost_efetch_to_xml(uids, batch_xml_path)
    base_rows = _xtract_headers(batch_xml_path)
    abs_map = _xtract_abstracts(batch_xml_path)

    # Merge and write
    written_count = 0
    for rec in base_rows:
        rec["abstract"] = abs_map.get(rec["pmid"], [])
        fout.write(json.dumps(rec, ensure_ascii=False) + "\n")
        written_count += 1
    return written_count



if __name__ == "__main__":
    uid_file = os.path.join(DATA_DIR, f"uid_{BEGIN_YEAR}_{END_YEAR}.txt")
    out_file = os.path.join(DATA_DIR, f"abstracts_{BEGIN_YEAR}_{END_YEAR}{FILE_SUFFIX}.jsonl")
    run(uid_file, out_file, batch=BATCH_SIZE, api_key=NCBI_API_KEY)