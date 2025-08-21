# plot the data distribution in a bar chart

import json
import matplotlib.pyplot as plt
import argparse

def plot_data_distribution(data, save_dir, threshold):
    """
    Plot data distribution for journals and labels in separate bar charts.
    
    Args:
        data: Dictionary containing journal_dict and label_dict
        save_dir: Directory to save the plots
        threshold: Dictionary with conclusion_num_threshold and label_num_threshold
    """
    import os
    
    # Ensure save directory exists
    os.makedirs(save_dir, exist_ok=True)
    
    # Get filtered data
    journal_dict = data["journal_dict"]
    conclusion_distribution = {k: v["conclusion_num"] for k, v in journal_dict.items() 
                             if v["conclusion_num"] >= threshold["conclusion_num_threshold"]}
    
    label_dict = data["label_dict"]
    label_distribution = {k: v for k, v in label_dict.items() 
                         if v >= threshold["label_num_threshold"]}
    
    # Plot 1: Journal conclusion distribution
    if conclusion_distribution:
        plt.figure(figsize=(12, 8))
        journals = list(conclusion_distribution.keys())
        counts = list(conclusion_distribution.values())
        
        plt.bar(journals, counts, color='skyblue', alpha=0.7)
        plt.title(f'Journal Distribution (Conclusion Count ≥ {threshold["conclusion_num_threshold"]})', 
                 fontsize=14, fontweight='bold')
        plt.xlabel('Journal', fontsize=12)
        plt.ylabel('Number of Conclusions', fontsize=12)
        plt.xticks(rotation=45, ha='right')
        plt.grid(axis='y', alpha=0.3)
        plt.tight_layout()
        
        # Save the plot
        plt.savefig(os.path.join(save_dir, 'journal_conclusion_distribution.png'), 
                   dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Journal distribution plot saved to {os.path.join(save_dir, 'journal_conclusion_distribution.png')}")
    
    # Plot 2: Label distribution
    if label_distribution:
        plt.figure(figsize=(12, 8))
        labels = list(label_distribution.keys())
        counts = list(label_distribution.values())
        
        plt.bar(labels, counts, color='lightcoral', alpha=0.7)
        plt.title(f'Label Distribution (Count ≥ {threshold["label_num_threshold"]})', 
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
    
    print(f"Found {len(conclusion_distribution)} journals with ≥{threshold['conclusion_num_threshold']} conclusions")
    print(f"Found {len(label_distribution)} labels with ≥{threshold['label_num_threshold']} occurrences")



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_path", type=str, required=True)
    parser.add_argument("--save_dir", type=str, default="data/")
    parser.add_argument("--conclusion_num_threshold", type=int, default=6000)
    parser.add_argument("--label_num_threshold", type=int, default=40000)
    args = parser.parse_args()

    with open(args.data_path, "r") as f:
        data = json.load(f)

    # Create threshold dictionary
    thresholds = {
        "conclusion_num_threshold": args.conclusion_num_threshold,
        "label_num_threshold": args.label_num_threshold
    }
    
    # Plot the data distribution
    plot_data_distribution(data, args.save_dir, thresholds)