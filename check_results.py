import json
from config import DATA_DIR, BEGIN_YEAR, END_YEAR, FILE_SUFFIX
import os
import argparse

CONCLUSION_LABEL = [
    "conclusion",
    "conclusions",
    "conclusion:",
    "conclusions:",
    "conclusion-",
    "conclusions-",
    "conclusion(s)",
    "conclusion.",
    "conclusions.",
    "author's conclusion",
    "author's conclusions",
    "authors' conclusion",
    "authors' conclusions",
    "conclusion and relevance",
    "conclusions and relevance",
    "conclusion and implications",
    "conclusions and implications",
    "conclusions/interpretation",
    "conclusion/interpretation",
    "conclusion/relevance",
]

CLIP_STHRESHOLD = 200

def check_data_item(record, statistics_dict, pmid_set):
    """
    A record is the json object in the following format:
    {
        "pmid": "37171444",
        "title": "Association of COVID-19 'circuit breaker' with higher rates of elderly trauma admissions.",
        "journal_title": "Singapore medical journal",
        "journal_iso": "Singapore Med J",
        "volume": "66",
        "issue": "2",
        "doi": "10.11622/smedj.2025.0001",
        "language": "eng",
        "pub_date": {"year": "2025", "month": "02", "day": "01"},
        "article_date": {"year": "2025", "month": "02", "day": "01"},
        "abstract": [
            {"label": "INTRODUCTION", "nlm_category": "NA", "text": "In December 2019, ..."},
            {"label": "METHODS", "nlm_category": "NA", "text": "An observational, ..."},
            {"label": "RESULTS", "nlm_category": "NA", "text": "A total of ..."},
            {"label": "CONCLUSION", "nlm_category": "NA", "text": "Our ..."}
        ],
        "keywords": ["COVID-19", "circuit breaker", "elderly trauma", "Singapore"]
    }
    """
    statistics_dict["data_size"] += 1
    
    pmid = record["pmid"]
    # check if pmid is already in the set
    if pmid in pmid_set:
        print(f"pmid {pmid} already exists")
        return
    else:
        pmid_set.add(pmid)
        statistics_dict["pmid_num"] += 1

    # check the title
    title = record["title"]
    if len(title) < 8:
        print(f"pubmed id {pmid}, title {title} is too short")
    
    # count the journal
    journal = record["journal_title"]
    if journal not in statistics_dict["journal_dict"]:
        statistics_dict["journal_dict"][journal] = {"num": 1, "conclusion_num": 0}
    else:
        statistics_dict["journal_dict"][journal]["num"] += 1
    
    # count the language
    language = record["language"]
    if language not in statistics_dict["language_dict"]:
        statistics_dict["language_dict"][language] = 1
    else:
        statistics_dict["language_dict"][language] += 1

    has_conclusion = False

    # count the label and nlm_category
    for abstract in record["abstract"]:
        label = abstract["label"]
        nlm_category = abstract["nlm_category"]
        if label not in statistics_dict["label_dict"]:
            statistics_dict["label_dict"][label] = 1
        else:
            statistics_dict["label_dict"][label] += 1
        if nlm_category not in statistics_dict["nlm_category_dict"]:
            statistics_dict["nlm_category_dict"][nlm_category] = 1
        else:
            statistics_dict["nlm_category_dict"][nlm_category] += 1
        # check if the conclusion label is in the abstract
        if label.lower() in CONCLUSION_LABEL or nlm_category.lower() in CONCLUSION_LABEL:
            has_conclusion = True
    if has_conclusion:
        statistics_dict["journal_dict"][journal]["conclusion_num"] += 1
        
    # count the keywords
    for keyword in record["keywords"]:
        if keyword not in statistics_dict["keywords_dict"]:
            statistics_dict["keywords_dict"][keyword] = 1
        else:
            statistics_dict["keywords_dict"][keyword] += 1
    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_path", type=str, default=None)
    parser.add_argument("--save_path", type=str, default=None)
    args = parser.parse_args()


    # get the data path
    if args.data_path is None or args.save_path is None:
        data_path = os.path.join(DATA_DIR, f"abstracts_{BEGIN_YEAR}_{END_YEAR}{FILE_SUFFIX}.jsonl")
        save_path = os.path.join(DATA_DIR, f"statistics_{BEGIN_YEAR}_{END_YEAR}{FILE_SUFFIX}.json")
    else:
        data_path = args.data_path
        save_path = args.save_path

    statistics_dict = {}
    statistics_dict["data_size"] = 0
    statistics_dict["pmid_num"] = 0
    pmid_set = set()
    statistics_dict["journal_dict"] = {}
    statistics_dict["label_dict"] = {}
    statistics_dict["nlm_category_dict"] = {}
    statistics_dict["language_dict"] = {}
    statistics_dict["keywords_dict"] = {}

    with open(data_path, "r") as f:
        for line in f:
            record = json.loads(line)
            check_data_item(record, statistics_dict, pmid_set)
            if statistics_dict["data_size"] % 10000 == 0:
                print(f"processed {statistics_dict['data_size']} records")

    # sort the journal_dict by the conclusion_num
    statistics_dict["journal_dict"] = dict(sorted(statistics_dict["journal_dict"].items(), key=lambda x: x[1]["conclusion_num"], reverse=True))
    # sort the label_dict by the num
    statistics_dict["label_dict"] = dict(sorted(statistics_dict["label_dict"].items(), key=lambda x: x[1], reverse=True))
    # sort the nlm_category_dict by the num
    statistics_dict["nlm_category_dict"] = dict(sorted(statistics_dict["nlm_category_dict"].items(), key=lambda x: x[1], reverse=True))
    # sort the language_dict by the num
    statistics_dict["language_dict"] = dict(sorted(statistics_dict["language_dict"].items(), key=lambda x: x[1], reverse=True))
    # sort the keywords_dict by the num
    statistics_dict["keywords_dict"] = dict(sorted(statistics_dict["keywords_dict"].items(), key=lambda x: x[1], reverse=True))

    # clip the list by the CLIP_STHRESHOLD
    statistics_dict["journal_dict"] = {k: v for k, v in statistics_dict["journal_dict"].items() if v["conclusion_num"] > CLIP_STHRESHOLD}
    statistics_dict["label_dict"] = {k: v for k, v in statistics_dict["label_dict"].items() if v > CLIP_STHRESHOLD}
    statistics_dict["keywords_dict"] = {k: v for k, v in statistics_dict["keywords_dict"].items() if v > CLIP_STHRESHOLD}

    # save the statistics_dict to the save_path
    with open(save_path, "w") as f:
        json.dump(statistics_dict, f, indent=4)
        print(f"saved the statistics_dict to the {save_path}")
