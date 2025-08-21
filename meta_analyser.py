# meta analyser
import json
import os
import argparse

# python meta_analyser.py --data_path abstracts/cleaned_data.jsonl --save_path abstracts/meta_analyser_result.json

frequency_threshold = 1000

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
    pmid = record["pmid"]
    # check if pmid is already in the set
    if pmid in pmid_set:
        raise ValueError(f"pmid {pmid} already exists")
    else:
        pmid_set.add(pmid)   
    statistics_dict["data_size"] += 1
    
    # count the journal
    journal = record["journal_title"]
    if journal not in statistics_dict["journal_dict"]:
        statistics_dict["journal_dict"][journal] = 1
    else:
        statistics_dict["journal_dict"][journal] += 1


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
        
    # count the keywords
    for keyword in record["keywords"]:
        if keyword not in statistics_dict["keywords_dict"]:
            statistics_dict["keywords_dict"][keyword] = 1
        else:
            statistics_dict["keywords_dict"][keyword] += 1
    return 


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_path", type=str, required=True)
    parser.add_argument("--save_path", type=str, required=True)
    args = parser.parse_args()


    # get the data path
    data_path = args.data_path
    if not os.path.exists(data_path):
        print(f"data_path {data_path} does not exist")
        exit(1)
    save_path = args.save_path
    save_dir = os.path.dirname(save_path)
    if save_dir != "" and not os.path.exists(save_dir):
        os.makedirs(save_dir)

    statistics_dict = {}
    statistics_dict["data_size"] = 0
    pmid_set = set()
    statistics_dict["journal_dict"] = {}
    statistics_dict["label_dict"] = {}
    statistics_dict["nlm_category_dict"] = {}
    statistics_dict["keywords_dict"] = {}

    with open(data_path, "r", encoding="utf-8") as f:
        for line in f:
            record = json.loads(line)
            check_data_item(record, statistics_dict, pmid_set)
            if statistics_dict["data_size"] % 100000 == 0:
                print(f"processed {statistics_dict['data_size']} records")

    # sort the journal_dict by the the number of records
    statistics_dict["journal_dict"] = dict(sorted(statistics_dict["journal_dict"].items(), key=lambda x: x[1], reverse=True))
    # sort the label_dict by the number of records
    statistics_dict["label_dict"] = dict(sorted(statistics_dict["label_dict"].items(), key=lambda x: x[1], reverse=True))
    # sort the nlm_category_dict by the number of records
    statistics_dict["nlm_category_dict"] = dict(sorted(statistics_dict["nlm_category_dict"].items(), key=lambda x: x[1], reverse=True))
    # sort the keywords_dict by the number of records
    statistics_dict["keywords_dict"] = dict(sorted(statistics_dict["keywords_dict"].items(), key=lambda x: x[1], reverse=True))

    # clip the list by the frequency_threshold
    statistics_dict["journal_dict"] = {k: v for k, v in statistics_dict["journal_dict"].items() if v >= frequency_threshold}
    statistics_dict["label_dict"] = {k: v for k, v in statistics_dict["label_dict"].items() if v >= frequency_threshold}
    statistics_dict["nlm_category_dict"] = {k: v for k, v in statistics_dict["nlm_category_dict"].items() if v >= frequency_threshold}
    statistics_dict["keywords_dict"] = {k: v for k, v in statistics_dict["keywords_dict"].items() if v >= frequency_threshold}

    # save the statistics_dict to the save_path
    with open(save_path, "w", encoding="utf-8") as f:
        json.dump(statistics_dict, f, indent=4, ensure_ascii=False)
        print(f"saved the statistics_dict to the {save_path}")
