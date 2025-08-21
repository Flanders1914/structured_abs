# data cleaner
# 1. remove the records that are not English
# 2. format any missing value as "NA"
# 3. format the dates: year must be valid 4 digits, month must be valid 2 digits, day must be valid 2 digits
# 4. remove the records that are not in English
# 5. remove the records that have less than 3 abstract elements
# 6. remove the records that does not have a conclusion label in the conclusion label list.
# 6. format the abstract labels and text, remove records that have non-ascii characters
# 7. format the keywords, remove records that have non-ascii characters


import json
import argparse

def clean_record(record) -> dict:
    
    # check the volume
    if record["volume"] == "":
        record["volume"] = "NA"
    # check the issue
    if record["issue"] == "":
        record["issue"] = "NA"
    # check the doi
    if record["doi"] == "":
        record["doi"] = "NA"
    # check the pub_date, if the pub year is not valid, return None
    pub_date = record["pub_date"]
    if pub_date["year"] == "" or len(pub_date["year"]) != 4 or not pub_date["year"].isdecimal():
        return None
    if pub_date["month"] == "" or len(pub_date["month"]) != 2 or not pub_date["month"].isdecimal():
        pub_date["month"] = "NA"
    if pub_date["day"] == "" or len(pub_date["day"]) != 2 or not pub_date["day"].isdecimal():
        pub_date["day"] = "NA"
    record["pub_date"] = pub_date
    # check the article_date
    article_date = record["article_date"]
    if article_date["year"] == "" or len(article_date["year"]) != 4 or not article_date["year"].isdecimal():
        article_date["year"] = "NA"
    if article_date["month"] == "" or len(article_date["month"]) != 2 or not article_date["month"].isdecimal():
        article_date["month"] = "NA"
    if article_date["day"] == "" or len(article_date["day"]) != 2 or not article_date["day"].isdecimal():
        article_date["day"] = "NA"
    record["article_date"] = article_date

    # check the abstract
    for abstract_element in record["abstract"]:
        # return None if an label or text is empty
        if abstract_element["label"] == "" or abstract_element["text"] == "":
            return None
        # check the label, if a label is too long or includes non-ascii characters, return None
        if len(abstract_element["label"]) > 10 or not abstract_element["label"].isascii():
            return None
        # check the text, if a text includes non-ascii characters, return None
        if not abstract_element["text"].isascii():
            return None
        # check the nlm_category, if it is empty or "UNASSIGNED", assign it to "NA"
        if abstract_element["nlm_category"] == "" or abstract_element["nlm_category"] == "UNASSIGNED":
            abstract_element["nlm_category"] = "NA"

    # check the keywords
    cleaned_keywords = []
    for keyword in record["keywords"]:
        # if a keyword is too long or includes non-ascii characters, discard it
        if len(keyword) > 10 or not keyword.isascii():
            continue
        cleaned_keywords.append(keyword)
    record["keywords"] = cleaned_keywords
    return record

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_path", type=str, required=True)
    parser.add_argument("--save_path", type=str, required=True)
    args = parser.parse_args()

    print(f"cleaning {args.data_path}...")
    count = 0
    discarded_records = 0
    cleaned_records = 0
    with open(args.data_path, "r", encoding="utf-8") as f, open(args.save_path, "w", encoding="utf-8") as fout:
        for line in f:
            record = json.loads(line)
            count += 1
            if count % 10000 == 0:
                print(f"processed {count} records")
            # check the pmid, title, journal_title, journal_iso, language
            if record["pmid"] == "" or record["title"] == "" or record["journal_title"] == "" or record["journal_iso"] == "" or record["language"] != "eng":
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
