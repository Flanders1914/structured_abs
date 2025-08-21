# de-duplicator
# deduplicate(by pmid, doi, title)
# python de_duplicator.py --data_path abstracts/raw_data.jsonl --save_path abstracts/dedup_data.jsonl
import json
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_path", type=str, required=True)
    parser.add_argument("--save_path", type=str, required=True)
    args = parser.parse_args()

    # deduplicate by the pmid, doi, title, check in lower case
    pmid_set = set()
    doi_set = set()
    title_set = set()
    count = 0
    write_count = 0
    with open(args.data_path, "r", encoding="utf-8") as fin, open(args.save_path, "w", encoding="utf-8") as fout:
        for line in fin:
            count += 1
            if count % 10000 == 0:
                print(f"processed {count} records")
            record = json.loads(line)

            pmid = record.get("pmid", "")
            title = record.get("title", "")
            doi = record.get("doi", "")

            pmid = pmid.strip()
            title = title.strip().lower()
            doi = doi.strip().lower()

            # skip records with empty pmid or title
            if pmid == "" or title == "":
                continue
            # some valid records have empty doi
            if pmid in pmid_set or title in title_set or (doi != "" and doi in doi_set):
                continue
            pmid_set.add(pmid)
            if doi != "":
                doi_set.add(doi)
            title_set.add(title)
            write_count += 1
            fout.write(json.dumps(record, ensure_ascii=False) + "\n")

    print(f"processed {count} records")
    print(f"saved {write_count} records to {args.save_path}")