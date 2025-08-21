# plot the data distribution in a bar chart

import json
import matplotlib.pyplot as plt
import argparse
import os

def plot_data_distribution(data, save_dir, thresholds):
    """
    Plot data distribution for journals, labels, nlm_categories, and keywords in separate bar charts.
    
    Args:
        data: Dictionary containing journal_dict, label_dict, nlm_category_dict, and keywords_dict
        save_dir: Directory to save the plots
        thresholds: Dictionary with different thresholds for each category
    """
    
    # Ensure save directory exists
    os.makedirs(save_dir, exist_ok=True)
    
    # Print data summary
    print(f"Total data size: {data.get('data_size', 'N/A')}")
    
    # Get the raw data dictionaries and apply different thresholds
    # Note: meta_analyser.py might have already filtered, so we get original or filtered data
    all_journal_dict = data.get("journal_dict", {})
    all_label_dict = data.get("label_dict", {})
    all_nlm_category_dict = data.get("nlm_category_dict", {})
    all_keywords_dict = data.get("keywords_dict", {})
    
    # Apply different thresholds for each category
    journal_dict = {k: v for k, v in all_journal_dict.items() if v >= thresholds["journal_threshold"]}
    label_dict = {k: v for k, v in all_label_dict.items() if v >= thresholds["label_threshold"]}
    nlm_category_dict = {k: v for k, v in all_nlm_category_dict.items() if v >= thresholds["nlm_category_threshold"]}
    keywords_dict = {k: v for k, v in all_keywords_dict.items() if v >= thresholds["keyword_threshold"]}
    
    # Plot 1: Journal distribution
    if journal_dict:
        plt.figure(figsize=(15, 8))
        journals = list(journal_dict.keys())
        counts = list(journal_dict.values())
        
        plt.bar(journals, counts, color='skyblue', alpha=0.7)
        plt.title(f'Journal Distribution (Frequency ≥ {thresholds["journal_threshold"]})', 
                 fontsize=14, fontweight='bold')
        plt.xlabel('Journal', fontsize=12)
        plt.ylabel('Number of Records', fontsize=12)
        plt.xticks(rotation=45, ha='right')
        plt.grid(axis='y', alpha=0.3)
        plt.tight_layout()
        
        # Save the plot
        plt.savefig(os.path.join(save_dir, 'journal_distribution.png'), 
                   dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Journal distribution plot saved to {os.path.join(save_dir, 'journal_distribution.png')}")
    
    # Plot 2: Label distribution
    if label_dict:
        plt.figure(figsize=(12, 8))
        labels = list(label_dict.keys())
        counts = list(label_dict.values())
        
        plt.bar(labels, counts, color='lightcoral', alpha=0.7)
        plt.title(f'Label Distribution (Frequency ≥ {thresholds["label_threshold"]})', 
                 fontsize=14, fontweight='bold')
        plt.xlabel('Label', fontsize=12)
        plt.ylabel('Count', fontsize=12)
        plt.xticks(rotation=45, ha='right')
        plt.grid(axis='y', alpha=0.3)
        plt.tight_layout()
        
        # Save the plot
        plt.savefig(os.path.join(save_dir, 'label_distribution.png'), 
                   dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Label distribution plot saved to {os.path.join(save_dir, 'label_distribution.png')}")
    
    # Plot 3: NLM Category distribution
    if nlm_category_dict:
        plt.figure(figsize=(12, 8))
        nlm_categories = list(nlm_category_dict.keys())
        counts = list(nlm_category_dict.values())
        
        plt.bar(nlm_categories, counts, color='lightseagreen', alpha=0.7)
        plt.title(f'NLM Category Distribution (Frequency ≥ {thresholds["nlm_category_threshold"]})', 
                 fontsize=14, fontweight='bold')
        plt.xlabel('NLM Category', fontsize=12)
        plt.ylabel('Count', fontsize=12)
        plt.xticks(rotation=45, ha='right')
        plt.grid(axis='y', alpha=0.3)
        plt.tight_layout()
        
        # Save the plot
        plt.savefig(os.path.join(save_dir, 'nlm_category_distribution.png'), 
                   dpi=300, bbox_inches='tight')
        plt.close()
        print(f"NLM category distribution plot saved to {os.path.join(save_dir, 'nlm_category_distribution.png')}")
    
    # Plot 4: Keyword distribution
    if keywords_dict:
        plt.figure(figsize=(15, 8))
        keywords = list(keywords_dict.keys())
        counts = list(keywords_dict.values())
        
        plt.bar(keywords, counts, color='lightgreen', alpha=0.7)
        plt.title(f'Keyword Distribution (Frequency ≥ {thresholds["keyword_threshold"]})', 
                 fontsize=14, fontweight='bold')
        plt.xlabel('Keyword', fontsize=12)
        plt.ylabel('Count', fontsize=12)
        plt.xticks(rotation=45, ha='right')
        plt.grid(axis='y', alpha=0.3)
        plt.tight_layout()
        
        # Save the plot
        plt.savefig(os.path.join(save_dir, 'keyword_distribution.png'), 
                   dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Keyword distribution plot saved to {os.path.join(save_dir, 'keyword_distribution.png')}")

    print(f"Found {len(journal_dict)} journals with frequency ≥ {thresholds['journal_threshold']}")
    print(f"Found {len(label_dict)} labels with frequency ≥ {thresholds['label_threshold']}")
    print(f"Found {len(nlm_category_dict)} NLM categories with frequency ≥ {thresholds['nlm_category_threshold']}")
    print(f"Found {len(keywords_dict)} keywords with frequency ≥ {thresholds['keyword_threshold']}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_path", type=str, required=True, 
                       help="Path to the JSON file output from meta_analyser.py")
    parser.add_argument("--save_dir", type=str, default="plots/", 
                       help="Directory to save the distribution plots")
    parser.add_argument("--journal_threshold", type=int, default=10000,
                       help="Minimum frequency threshold for journals")
    parser.add_argument("--label_threshold", type=int, default=100000,
                       help="Minimum frequency threshold for labels")
    parser.add_argument("--nlm_category_threshold", type=int, default=0,
                       help="Minimum frequency threshold for NLM categories")
    parser.add_argument("--keyword_threshold", type=int, default=1000,
                       help="Minimum frequency threshold for keywords")
    args = parser.parse_args()

    # Check if data file exists
    if not os.path.exists(args.data_path):
        print(f"Error: Data file {args.data_path} does not exist")
        exit(1)

    # Load the statistics data from meta_analyser.py output
    with open(args.data_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    # Create thresholds dictionary
    thresholds = {
        "journal_threshold": args.journal_threshold,
        "label_threshold": args.label_threshold,
        "nlm_category_threshold": args.nlm_category_threshold,
        "keyword_threshold": args.keyword_threshold
    }
    
    # Plot the data distribution
    plot_data_distribution(data, args.save_dir, thresholds)