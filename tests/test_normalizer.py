"""
Test script for schema normalizer.
Run from project root: python test_normalizer.py
"""

from pathlib import Path
from src.ingestion.dataset_loader import create_loader
from src.ingestion.schema_normalizer import SchemaNormalizer

def main():
    print("Testing schema normalizer...")
    
    # Create loader
    loader = create_loader(Path.cwd())
    
    # Load a small sample
    cases = loader.get_available_cases()
    if not cases:
        print("No cases found")
        return
    
    test_case = cases[0]
    print(f"Testing with case: {test_case}")
    
    # Load metrics
    metrics_df = loader.load_metrics(case_id=test_case, lazy=False)
    print(f"Loaded {len(metrics_df)} metric rows")
    
    # Normalize
    normalizer = SchemaNormalizer()
    normalized = normalizer.normalize_metrics(metrics_df)
    print(f"Normalized schema: {normalized.columns}")
    print(f"Sample:\n{normalized.head(3)}")
    
    print("\nTest completed successfully!")

if __name__ == "__main__":
    main()