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
```
# in config.py,
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