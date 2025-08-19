#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
get_abstracts.py (xtract-based)

Read local PubMed UIDs (one PMID per line), fetch PubMed XML in batches via
EDirect (epost | efetch -format xml), parse fields with custom parser
and write one JSON object per line (JSONL).
"""

import gc
import os
import json
import time
import subprocess
from typing import List, Dict, Optional

from tqdm import tqdm
from lxml import etree
from config import BATCH_SIZE, NCBI_API_KEY, DATA_DIR, BEGIN_YEAR, END_YEAR, FILE_SUFFIX

# ----------------------- Custom parser -----------------------
XML_PATH = {
    "pmid": etree.XPath('normalize-space(string(./MedlineCitation/PMID))', smart_strings=False),
    "title": etree.XPath('normalize-space(string(./MedlineCitation/Article/ArticleTitle))', smart_strings=False),
    "journal_title": etree.XPath('normalize-space(string(./MedlineCitation/Article/Journal/Title))', smart_strings=False),
    "journal_iso": etree.XPath('normalize-space(string(./MedlineCitation/Article/Journal/ISOAbbreviation))', smart_strings=False),
    "volume": etree.XPath('normalize-space(string(./MedlineCitation/Article/Journal/JournalIssue/Volume))', smart_strings=False),
    "issue": etree.XPath('normalize-space(string(./MedlineCitation/Article/Journal/JournalIssue/Issue))', smart_strings=False),
    "elocation_id_doi": etree.XPath('normalize-space(string(./MedlineCitation/Article/ELocationID[@EIdType="doi"]))', smart_strings=False),
    "language": etree.XPath('normalize-space(string(./MedlineCitation/Article/Language))', smart_strings=False),
    "pub_date_day": etree.XPath('normalize-space(string(./MedlineCitation/Article/Journal/JournalIssue/PubDate/Day))', smart_strings=False),
    "pub_date_month": etree.XPath('normalize-space(string(./MedlineCitation/Article/Journal/JournalIssue/PubDate/Month))', smart_strings=False),
    "pub_date_year": etree.XPath('normalize-space(string(./MedlineCitation/Article/Journal/JournalIssue/PubDate/Year))', smart_strings=False),
    "article_date_day": etree.XPath('normalize-space(string(./MedlineCitation/Article/ArticleDate[@DateType="Electronic"]/Day))', smart_strings=False),
    "article_date_month": etree.XPath('normalize-space(string(./MedlineCitation/Article/ArticleDate[@DateType="Electronic"]/Month))', smart_strings=False),
    "article_date_year": etree.XPath('normalize-space(string(./MedlineCitation/Article/ArticleDate[@DateType="Electronic"]/Year))', smart_strings=False),
    # the following may be multiple, so we need to return a list
    "abstract_nodes": etree.XPath('./MedlineCitation/Article/Abstract/AbstractText'),
    "keywords": etree.XPath('./MedlineCitation/KeywordList/Keyword/text()', smart_strings=False),
    # fallback for doi
    "doi_fallback": etree.XPath('normalize-space(string(./PubmedData/ArticleIdList/ArticleId[@IdType="doi"]))', smart_strings=False),
}




# Progress bar with ETA (tqdm); fallback to simple prints if not available.

MONTH_MAP = {
    # only lowercase
    "jan": "01", "feb": "02", "mar": "03", "apr": "04",
    "may": "05", "jun": "06", "jul": "07", "aug": "08",
    "sep": "09", "oct": "10", "nov": "11", "dec": "12",
    "january": "01", "february": "02", "march": "03", "april": "04",
    "may": "05", "june": "06", "july": "07", "august": "08",
    "september": "09", "october": "10", "november": "11", "december": "12",
    # unusual cases
    "sept": "09",
}

# ----------------------- Small helpers -----------------------
def _count_lines(path: str) -> int:
    """Count lines (each should contain one PMID)."""
    n = 0
    with open(path, "r", encoding="utf-8") as f:
        for _ in f:
            n += 1
    return n

# ----------------------- Core xtract steps -----------------------

def _epost_efetch_to_xml(uids: List[str], xml_path: str) -> None:
    """
    Send UIDs to epost, then efetch XML into a temp file.
    Uses one network call per batch. We reuse the same XML file for multiple passes.
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


# ----------------------- Custom parser -----------------------
def _xml_parser(xml_path: str) -> List[Dict]:
    """
    Parse the XML file and return a list of dictionaries, each dictionary is a record of the parsed data.
    """
    results = []
    context = etree.iterparse(xml_path,
        events=("end",),
        tag="PubmedArticle",
        load_dtd=False,
        no_network=True,
        remove_blank_text=True,
        remove_comments=True,
        remove_pis=True,
        huge_tree=True,
        recover=True,
    )
    # parse the xml file
    for _, element in context:
        record = _extract_abstract_items(element)
        if record is not None:
            results.append(record)
        element.clear()
        while element.getprevious() is not None:
            del element.getparent()[0]
    del context
    gc.collect()
    return results


def _extract_abstract_items(element: etree.Element) -> Dict:
    record = {}
    # single value
    record["pmid"] = XML_PATH["pmid"](element)
    record["title"] = XML_PATH["title"](element)
    record["journal_title"] = XML_PATH["journal_title"](element)
    record["journal_iso"] = XML_PATH["journal_iso"](element)
    record["volume"] = XML_PATH["volume"](element)
    record["issue"] = XML_PATH["issue"](element)
    record["doi"] = XML_PATH["elocation_id_doi"](element) or XML_PATH["doi_fallback"](element)
    record["language"] = XML_PATH["language"](element)
    record["pub_date"] = {}
    record["pub_date"]["day"] = XML_PATH["pub_date_day"](element)
    record["pub_date"]["month"] = XML_PATH["pub_date_month"](element)
    if record["pub_date"]["month"] and record["pub_date"]["month"].lower() in MONTH_MAP:
        record["pub_date"]["month"] = MONTH_MAP[record["pub_date"]["month"].lower()]
    record["pub_date"]["year"] = XML_PATH["pub_date_year"](element)
    record["article_date"] = {}
    record["article_date"]["day"] = XML_PATH["article_date_day"](element)
    record["article_date"]["month"] = XML_PATH["article_date_month"](element)
    if record["article_date"]["month"] and record["article_date"]["month"].lower() in MONTH_MAP:
        record["article_date"]["month"] = MONTH_MAP[record["article_date"]["month"].lower()]
    record["article_date"]["year"] = XML_PATH["article_date_year"](element)
    # multiple values
    abstract_nodes = XML_PATH["abstract_nodes"](element)
    keywords = XML_PATH["keywords"](element)
    record["abstract"] = []
    for abstract_node in abstract_nodes:
        label = (abstract_node.get("Label") or "").strip()
        nlm_category = (abstract_node.get("NlmCategory") or "").strip()
        text = "".join(abstract_node.itertext()).strip()
        if label == "":
            return None
        record["abstract"].append({"label": label, "nlm_category": nlm_category, "text": text})
    # keywords
    record["keywords"] = []
    for keyword in keywords:
        record["keywords"].append(keyword.strip())
    return record

# ----------------------- Runner -----------------------

def run(uid_path: str, out_path: str, batch: int = 8000, api_key: Optional[str] = None):
    """
    Read UIDs, fetch XML by batch via EDirect, parse with custom parser, and write JSONL.
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
    # set to avoid duplicates
    uid_set = set()

    with open(uid_path, "r", encoding="utf-8") as fin, open(tmp_out, "w", encoding="utf-8") as fout:
        chunk: List[str] = []
        for line in fin:
            pmid = line.strip()
            if not pmid:
                continue
            if pmid in uid_set:
                continue
            uid_set.add(pmid)
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
    """Fetch one batch to an XML file, run custom parser, then emit JSONL."""
    batch_xml_path = "temp/batch.xml"
    # create the temp directory if not exists
    os.makedirs("temp", exist_ok=True)
    # if batch_xml_path exists, clear it, else create it
    if os.path.exists(batch_xml_path):
        os.remove(batch_xml_path)
    # fetch xml
    _epost_efetch_to_xml(uids, batch_xml_path)
    # parse
    records = _xml_parser(batch_xml_path)
    # write
    print(f"Writing {len(records)} records to {fout}")
    written_count = 0
    for rec in records:
        fout.write(json.dumps(rec, ensure_ascii=False) + "\n")
        written_count += 1
    return written_count


if __name__ == "__main__":
    # test
    # uid_file = "data/uid_test.txt"
    # out_file = "data/abstracts_test.jsonl"

    uid_file = os.path.join(DATA_DIR, f"uid_{BEGIN_YEAR}_{END_YEAR}.txt")
    out_file = os.path.join(DATA_DIR, f"abstracts_{BEGIN_YEAR}_{END_YEAR}{FILE_SUFFIX}.jsonl")
    run(uid_file, out_file, batch=BATCH_SIZE, api_key=NCBI_API_KEY)