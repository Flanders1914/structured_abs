# PubMed Structured Abstract Downloading Tool

## Setup Environment
```bash
cd ~/structured_abs
python3 -m virtualenv ./myvenv
source myvenv/bin/activate
pip install requirements.txt
```

## Download Entrez Direct(PubMed API Tool) and Setup Env
```bash
cd ~
sh -c "$(curl -fsSL https://ftp.ncbi.nlm.nih.gov/entrez/entrezdirect/install-edirect.sh)"
export PATH=${HOME}/edirect:${PATH}
cd ~/structured_abs
```
The installation process ends by offering to run the PATH update command for you. Answer "y" and press the Return key if you want it run

## Setup config
```python
# in config.py,
DATA_DIR = "data/" # the directory to store results
BEGIN_YEAR = 2024 # Any begin year
END_YEAR = 2025 # Any end year
NCBI_API_KEY = "" # your NCBI API Key
BATCH_SIZE = 8000 # max 10000
```

## Run the Data Collection Pipeline

```bash
chmod +x ./get_uid.py
chmod +x ./get_abstracts_custom_parser.py
python ./get_uid.py
python ./get_abstracts_custom_parser.py
```

## Merge Data into One File
```python
# in merge_results.py. Confgure the file_list and the target_file
file_list = [
    "data/abstracts_2000_2004.jsonl",
    "data/abstracts_2005_2014.jsonl",
    "data/abstracts_2015_2017.jsonl",
    "data/abstracts_2018_2019.jsonl",
    "data/abstracts_2020_2023.jsonl",
    "data/abstracts_2024_2025.jsonl",
]

target_file = "abstracts/raw_data.jsonl"
```
```bash
python merge_results.py
```


## Dedeuplicate Data
```bash
python de_duplicator.py --data_path abstracts/raw_data.jsonl --save_path abstracts/dedup_data.jsonl
```

## Clean Data
The data cleaner only keep abstracts with at least 3 abstract elements with at least one conclusion label
A label is considered as conclusion label if it is in the following list:
```python
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
```
Note: "&" is replaced by "AND" and ":" is removed before checking if the label in the list.

```bash
python data_cleaner.py --data_path abstracts/dedup_data.jsonl --save_path abstracts/cleaned_data.jsonl
```

## Analyse Data
```bash
python meta_analyser.py --data_path abstracts/cleaned_data.jsonl --save_path abstracts/meta_analyser_result.json
```

### Plot Distribution
```bash
python plot_data_distribution.py --data_path abstracts/meta_analyser_result.json --save_dir plots/
```
Minimum frequency threshold can be configured optionally by "--journal_threshold, ----label_threshold, --nlm_category_threshold, --keyword_threshold"