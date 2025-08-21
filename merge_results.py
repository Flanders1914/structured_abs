# transfer the data in the file list to the target file
import os

file_list = [
    "data/abstracts_2000_2004.jsonl",
    "data/abstracts_2005_2014.jsonl",
    "data/abstracts_2015_2017.jsonl",
    "data/abstracts_2018_2019.jsonl",
    "data/abstracts_2020_2023.jsonl",
    "data/abstracts_2024_2025.jsonl",
]

target_file = "abstracts/raw_data.jsonl"

if __name__ == "__main__":

    # check if the path in the file_list is valid
    for file in file_list:
        if not os.path.exists(file):
            print(f"file {file} does not exist")
            exit(1)
    # create the target file's parent directory if it does not exist
    os.makedirs(os.path.dirname(target_file), exist_ok=True)

    print("start merging")
    count = 0
    with open(target_file, "w", encoding="utf-8") as fout:
        for file in file_list:
            print(f"processing {file}")
            with open(file, "r", encoding="utf-8") as fin:
                for line in fin:
                    if not line.endswith("\n"):
                        print(f"line {line} does not end with \\n")
                        line += "\n"
                    fout.write(line)
                    count += 1
                    if count % 10000 == 0:
                        print(f"processed {count} lines")
    print(f"merge done, {count} lines")