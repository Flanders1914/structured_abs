# select_records.py

import json
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_path", type=str, required=True)
    parser.add_argument("--save_path", type=str, required=True)
    parser.add_argument("--journal", type=str, required=True, help="Journal name")
    args = parser.parse_args()

    processed_count = 0
    selected_count = 0
    with open(args.data_path, "r", encoding="utf-8") as fin, open(args.save_path, "w", encoding="utf-8") as fout:
        for line in fin:
            record = json.loads(line)
            if record["journal_title"] == args.journal:
                fout.write(line)
                selected_count += 1
            processed_count += 1
            if processed_count % 100000 == 0:
                print(f"processed {processed_count} records")

    print(f"selected {selected_count} records")