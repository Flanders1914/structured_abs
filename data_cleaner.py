# data cleaner
# 1. remove the records that are not English
# 2. format any missing value as "NA"
# 3. format the dates: year must be valid 4 digits, month must be valid 2 digits, day must be valid 2 digits
# 4. remove the records that are not in English
# 5. remove the records that have less than 3 abstract elements
# 6. remove the records that does not have a conclusion label in the conclusion label list.
# 7. format the abstract labels and text, remove records that have non-ascii characters in labels or the labels are too long
# 8. format the keywords, remove records that have non-ascii characters or are too long

# python data_cleaner.py --data_path abstracts/dedup_data.jsonl --save_path abstracts/cleaned_data.jsonl
import json
import argparse
import os

CONCLUSION_LABELS = [
    "CONCLUSION",
    "CONCLUSIONS",
    "CONCLUSION(S)",
    "CONCLUSIONS AND RELEVANCE",
    "CONCLUSION AND RELEVANCE",
    "CONCLUSIONS AND IMPLICATIONS",
    "CONCLUSION AND IMPLICATIONS",
    "CONCLUSIONS AND IMPORTANCE",
    "CONCLUSION AND IMPORTANCE",
    "CONCLUSION AND SIGNIFICANCE",
    "CONCLUSIONS AND SIGNIFICANCE",
    "CONCLUSION AND INTERPRETATION",
    "CONCLUSIONS AND INTERPRETATION",
    "CONCLUSIONS AND CLINICAL RELEVANCE",
    "CONCLUSION AND CLINICAL RELEVANCE",
    "CONCLUSIONS AND CLINICAL IMPORTANCE",
    "CONCLUSION AND CLINICAL IMPORTANCE",
    "AUTHORS' CONCLUSIONS",
    "AUTHORS' CONCLUSION",
    "MAIN CONCLUSIONS",
    "MAIN CONCLUSION",
]

LABEL_LENGTH_LIMIT = 50
KEYWORD_LENGTH_LIMIT = 50

def _to_na_if_empty(s: str) -> str:
    s = s.strip()
    return s if s != "" else "NA"

def _norm_year(s: str) -> str:
    s = s.strip()
    if not s.isdecimal():
        return "NA"
    s = s.zfill(4)
    return s if len(s) == 4 else "NA"

def _norm_mm(s: str) -> str:
    s = s.strip()
    if not s.isdecimal():
        return "NA"
    n = int(s)
    return f"{n:02d}" if 1 <= n <= 12 else "NA"

def _norm_dd(s: str) -> str:
    s = s.strip()
    if not s.isdecimal():
        return "NA"
    n = int(s)
    return f"{n:02d}" if 1 <= n <= 31 else "NA"


def clean_record(record) -> dict:
    # check the volume
    record["volume"] = _to_na_if_empty(record["volume"])
    # check the issue
    record["issue"] = _to_na_if_empty(record["issue"])
    # check the doi
    record["doi"] = _to_na_if_empty(record["doi"])

    # check the pub_date
    pub_date = record["pub_date"]
    pub_date["year"] = _norm_year(pub_date["year"])
    pub_date["month"] = _norm_mm(pub_date["month"])
    pub_date["day"] = _norm_dd(pub_date["day"])
    record["pub_date"] = pub_date

    # check the article_date
    article_date = record["article_date"]
    article_date["year"] = _norm_year(article_date["year"])
    article_date["month"] = _norm_mm(article_date["month"])
    article_date["day"] = _norm_dd(article_date["day"])
    record["article_date"] = article_date

    # check the abstract
    conclusion_label_found = False
    # ensure the abstract has at least 3 elements
    if len(record["abstract"]) < 3:
        return None
    for abstract_element in record["abstract"]:
        label = abstract_element["label"].strip().upper()
        nlm_category = abstract_element["nlm_category"].strip().upper()
        text = abstract_element["text"].strip()
        # remove :, replace & with AND in the label
        label = label.replace(":", "").replace("&", "AND")
        # return None if an label or text is empty
        if label == "" or text == "":
            return None
        # check the label, if a label is too long or includes non-ascii characters, return None
        if len(label) > LABEL_LENGTH_LIMIT or not label.isascii():
            return None
        # check if the label is in the CONCLUSION_LABELS
        if label in CONCLUSION_LABELS:
            conclusion_label_found = True
        # check the nlm_category, if it is empty or "UNASSIGNED", assign it to "NA"
        if nlm_category == "" or nlm_category == "UNASSIGNED":
            nlm_category = "NA"
        abstract_element["label"] = label
        abstract_element["nlm_category"] = nlm_category
        abstract_element["text"] = text
    
    # if no conclusion label is found, return None
    if not conclusion_label_found:
        return None

    # check the keywords
    cleaned_keywords = []
    for keyword in record["keywords"]:
        keyword = keyword.strip()
        # if a keyword is empty or is too long
        if keyword == "" or len(keyword) > KEYWORD_LENGTH_LIMIT or not keyword.isascii():
            continue
        cleaned_keywords.append(keyword)
    record["keywords"] = cleaned_keywords
    return record


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_path", type=str, required=True)
    parser.add_argument("--save_path", type=str, required=True)
    args = parser.parse_args()

    # check the output directory
    output_dir = os.path.dirname(args.save_path)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # start cleaning
    print(f"cleaning {args.data_path}...")
    processed_count = 0
    discarded_records = 0
    cleaned_records = 0
    with open(args.data_path, "r", encoding="utf-8") as f, open(args.save_path, "w", encoding="utf-8") as fout:
        for line in f:
            record = json.loads(line)
            processed_count += 1
            if processed_count % 100000 == 0:
                print(f"processed {processed_count} records")
            # check the pmid, title, journal_title, journal_iso, language
            if record["pmid"] == "" or record["title"] == "" or record["journal_title"] == "" or record["journal_iso"] == "" or record["language"].lower() != "eng":
                discarded_records += 1
                continue
            cleaned_record = clean_record(record)
            if cleaned_record is None:
                discarded_records += 1
                continue
            fout.write(json.dumps(cleaned_record, ensure_ascii=False) + "\n")
            cleaned_records += 1

    print(f"discarded {discarded_records} records")
    print(f"cleaned {cleaned_records} records")
