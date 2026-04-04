"""
MASTER ORCHESTRATOR - Execute Phases 2-6 in Sequence
Builds a complete 20-25 GB mega dataset
"""

import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path


class MegaDatasetOrchestrator:
    """Orchestrate all dataset expansion phases"""
    
    def __init__(self):
        self.phases = []
        self.results = {}
        self.start_time = datetime.now()
    
    def run_phase(self, phase_num, script_name, description):
        """Run a single phase"""
        print("\n" + "="*100)
        print(f"▶ EXECUTING PHASE {phase_num}: {description}")
        print("="*100)
        
        try:
            phase_start = time.time()
            
            # Run the phase script
            result = subprocess.run(
                [sys.executable, script_name],
                cwd=Path(__file__).parent,
                capture_output=True,
                text=True
            )
            
            phase_duration = (time.time() - phase_start) / 60
            
            if result.returncode == 0:
                print("\n✅ Phase complete!")
                self.results[phase_num] = {
                    'status': 'SUCCESS',
                    'duration_minutes': phase_duration,
                    'script': script_name
                }
                return True
            else:
                print(f"\n❌ Phase failed!")
                print(result.stderr)
                self.results[phase_num] = {
                    'status': 'FAILED',
                    'duration_minutes': phase_duration,
                    'error': result.stderr
                }
                return False
                
        except Exception as e:
            print(f"\n❌ Error running phase: {e}")
            self.results[phase_num] = {
                'status': 'ERROR',
                'error': str(e)
            }
            return False
    
    def execute_all_phases(self, skip_phases=None):
        """Execute phases 2-6 in sequence"""
        if skip_phases is None:
            skip_phases = []
        
        print("\n" + "="*100)
        print("🚀 MEGA DATASET BUILDER - PHASES 2-6")
        print("="*100)
        print(f"\nStart time: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("\nPhases to execute:")
        print("  2. Expand to 300+ global stocks     - 200+ new stocks")
        print("  3. Add hourly data (optional)       - Top 100 stocks")
        print("  4. Technical indicators            - 25+ indicators per stock")
        print("  5. Sentiment scores                - 5 sentiment columns")
        print("  6. Macro indicators                - 15 macro columns")
        
        phases_config = [
            (2, 'phase2_expand_stocks.py', 'Expand to 300+ Global Stocks'),
            (3, 'download_intraday_data.py', 'Add Hourly Candles (Optional)'),
            (4, 'phase4_technical_indicators.py', 'Generate Technical Indicators'),
            (5, 'phase5_sentiment_scores.py', 'Add Sentiment Scores'),
            (6, 'phase6_macro_indicators.py', 'Add Macro Indicators'),
        ]
        
        # Execute phases
        for phase_num, script, description in phases_config:
            if phase_num in skip_phases:
                print(f"\n⏭️  Skipping Phase {phase_num}: {description}")
                continue
            
            if not self.run_phase(phase_num, script, description):
                # Ask if user wants to continue
                response = input(f"\nPhase {phase_num} failed. Continue? (y/n): ")
                if response.lower() != 'y':
                    print("Stopping execution.")
                    break
        
        # Final summary
        self.print_summary()
    
    def print_summary(self):
        """Print final summary"""
        total_time = (datetime.now() - self.start_time).total_seconds() / 60
        
        print("\n" + "="*100)
        print("📊 FINAL SUMMARY")
        print("="*100)
        
        successful = sum(1 for r in self.results.values() if r.get('status') == 'SUCCESS')
        failed = sum(1 for r in self.results.values() if r.get('status') != 'SUCCESS')
        
        print(f"\nPhases completed: {successful}")
        print(f"Phases failed: {failed}")
        print(f"Total time: {total_time:.1f} minutes\n")
        
        for phase_num, result in self.results.items():
            status_icon = "✅" if result['status'] == 'SUCCESS' else "❌"
            duration = result.get('duration_minutes', 0)
            print(f"  {status_icon} Phase {phase_num}: {result['status']} ({duration:.1f}m)")
        
        if successful == len(self.results):
            print("\n🎉 ALL PHASES COMPLETED SUCCESSFULLY!")
            print("\nYour mega dataset is ready!")
            print("✓ ~300+ stocks with daily data")
            print("✓ Technical indicators (25+ per stock)")
            print("✓ Sentiment scores (5 per stock)")
            print("✓ Macro indicators (15 per stock)")
            print("✓ Optional: Hourly data for top 100 stocks")
            print("\nEstimated dataset size: 15-25 GB")
            
            print("\n📚 Files created:")
            data_dir = Path('data')
            csv_files = list(data_dir.rglob('*.csv'))
            print(f"  Total CSV files: {len(csv_files)}")
            total_size_gb = sum(f.stat().st_size for f in csv_files) / (1024**3)
            print(f"  Total data size: {total_size_gb:.2f} GB")
            
            print("\n🎯 Next steps:")
            print("  1. Run: python explore_mega_dataset.py")
            print("  2. Train new models with: python train_mega_model.py")
            print("  3. Make predictions with: python predict_mega_stocks.py")


def main():
    """Main execution"""
    orchestrator = MegaDatasetOrchestrator()
    
    # Ask which phases to run
    print("\n" + "="*100)
    print("SELECT PHASES TO EXECUTE")
    print("="*100)
    print("\nOptions:")
    print("  1. All phases (2-6)                 - Full mega dataset")
    print("  2. Skip Phase 3 (2,4,5,6)           - No hourly data")
    print("  3. Phases 2 & 4 only                - Stocks + Technicals")
    print("  4. Custom selection")
    
    choice = input("\nEnter choice (1-4): ").strip()
    
    skip_phases = []
    if choice == "1":
        print("\n✓ Running all phases...")
    elif choice == "2":
        print("\n✓ Skipping Phase 3 (hourly data)...")
        skip_phases = [3]
    elif choice == "3":
        print("\n✓ Running Phases 2 & 4 only...")
        skip_phases = [3, 5, 6]
    elif choice == "4":
        skip_input = input("Enter phases to skip (e.g., 3,5): ").strip()
        skip_phases = [int(x.strip()) for x in skip_input.split(',') if x.strip()]
    
    # Execute
    orchestrator.execute_all_phases(skip_phases)


if __name__ == "__main__":
    main()
