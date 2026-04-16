"""
Train Model

Manual script to train and save prediction models.

Usage:
    python scripts/train_model.py [--start 2021-09-01] [--end 2026-03-31] [--model lightgbm]
"""

import argparse
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import settings
from config.logging_config import setup_logging
from db.database import init_database, get_session
from agents.predictor.model_trainer import ModelTrainer


def main():
    parser = argparse.ArgumentParser(description="Train prediction models")
    parser.add_argument("--start", type=str, default="2021-09-01", help="Training start date")
    parser.add_argument("--end", type=str, default="2026-03-31", help="Training end date")
    parser.add_argument("--model", type=str, default="lightgbm", choices=["lightgbm", "xgboost"])
    parser.add_argument("--target", type=str, default="win", choices=["win", "place", "both"])
    args = parser.parse_args()

    setup_logging(settings)
    init_database()

    session = get_session()
    trainer = ModelTrainer(session)

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)

    if args.target in ("win", "both"):
        print(f"\nTraining WIN model ({args.model})...")
        model, metadata = trainer.train_win_model(start, end, args.model)
        if model:
            version = trainer.save_model(model, metadata)
            print(f"WIN model saved: {version}")
            print(f"  AUC: {metadata.get('validation_auc', 0):.4f}")
            print(f"  Training races: {metadata.get('training_races_count', 0)}")

    if args.target in ("place", "both"):
        print(f"\nTraining PLACE model ({args.model})...")
        model, metadata = trainer.train_place_model(start, end, args.model)
        if model:
            version = trainer.save_model(model, metadata)
            print(f"PLACE model saved: {version}")

    session.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
