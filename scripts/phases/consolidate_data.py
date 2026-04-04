"""
DATA CONSOLIDATION: Move all scattered stock files into single data/ folder
Consolidates from: data/phase9/*, data/india/*, data/usa/*, data/europe/, etc.
Result: All CSVs in root data/ folder with unified naming
"""

import os
import shutil
import glob
from pathlib import Path
import time

def consolidate_data():
    """Consolidate all scattered data into root data folder"""
    print("\n" + "="*100)
    print("DATA CONSOLIDATION: UNIFIED DATA FOLDER")
    print("="*100)
    print("\nConsolidating all stock files to single data/ folder...\n")
    
    # Root data folder
    root_data = 'data'
    
    # Find all CSV files in subdirectories
    all_csv_files = glob.glob(f'{root_data}/**/*.csv', recursive=True)
    
    # Filter out files already in root
    files_to_move = [f for f in all_csv_files if os.path.dirname(f) != root_data]
    
    print(f"Found {len(files_to_move)} files in subdirectories")
    print(f"Found {len([f for f in all_csv_files if os.path.dirname(f) == root_data])} files already in root\n")
    
    # Track statistics
    moved = 0
    skipped = 0
    renamed = 0
    
    # Move files
    print("Moving files to root data/ folder...\n")
    for filepath in files_to_move:
        try:
            filename = os.path.basename(filepath)
            dest_path = os.path.join(root_data, filename)
            
            # If file exists, rename it
            if os.path.exists(dest_path):
                name, ext = os.path.splitext(filename)
                # Add subdirectory prefix to avoid conflicts
                subdir = os.path.dirname(filepath).replace(f'{root_data}/', '').replace('\\', '_')
                new_filename = f"{name}_{subdir}{ext}"
                dest_path = os.path.join(root_data, new_filename)
                renamed += 1
            
            # Move file
            shutil.move(filepath, dest_path)
            moved += 1
            
            if moved % 100 == 0:
                print(f"  Moved {moved} files...")
        
        except Exception as e:
            skipped += 1
            print(f"  Error moving {filepath}: {str(e)[:50]}")
    
    print(f"\n  Completed! Moved {moved}, Renamed {renamed}, Skipped {skipped}")
    
    # Remove empty subdirectories
    print("\nRemoving empty subdirectories...")
    subdirs = [d for d in glob.glob(f'{root_data}/**/') if os.path.isdir(d)]
    removed_dirs = 0
    
    for subdir in sorted(subdirs, reverse=True):
        try:
            if not os.listdir(subdir) and subdir != root_data + '/':
                os.rmdir(subdir)
                removed_dirs += 1
        except:
            pass
    
    print(f"  Removed {removed_dirs} empty directories")
    
    # Final statistics
    final_files = len(glob.glob(f'{root_data}/*.csv'))
    final_size_gb = sum(os.path.getsize(f) for f in glob.glob(f'{root_data}/*.csv')) / (1024**3)
    
    print(f"\n{'='*100}")
    print(f"CONSOLIDATION SUMMARY")
    print(f"{'='*100}\n")
    print(f"✓ Files moved to root: {moved}")
    print(f"✓ Files renamed (conflicts): {renamed}")
    print(f"✓ Empty dirs removed: {removed_dirs}")
    print(f"✓ Total files in data/: {final_files}")
    print(f"✓ Total size: {final_size_gb:.2f} GB")
    print(f"\n✓ All stock data now consolidated in data/ folder!")
    print(f"{'='*100}\n")

if __name__ == "__main__":
    start = time.time()
    consolidate_data()
    elapsed = time.time() - start
    print(f"Time: {elapsed/60:.1f} minutes\n")
