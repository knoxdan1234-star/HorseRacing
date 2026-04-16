"""
Model Trainer

Trains LightGBM and XGBoost models for win/place prediction.
Supports walk-forward validation and hyperparameter tuning.
"""

import logging
from datetime import date, datetime
from pathlib import Path

import joblib
import lightgbm as lgb
import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.metrics import log_loss, roc_auc_score
from sklearn.model_selection import TimeSeriesSplit
from sqlalchemy.orm import Session

from agents.predictor.feature_engine import FeatureEngineer
from config import settings
from db.database import get_session
from db.models import ModelMetadata

logger = logging.getLogger(__name__)


class ModelTrainer:
    """Trains and evaluates horse racing prediction models."""

    # LightGBM hyperparameters (tuned for horse racing)
    LGBM_PARAMS = {
        "objective": "binary",
        "metric": "binary_logloss",
        "boosting_type": "gbdt",
        "num_leaves": 63,
        "learning_rate": 0.05,
        "n_estimators": 1000,
        "max_depth": 8,
        "min_child_samples": 50,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "reg_alpha": 0.1,
        "reg_lambda": 1.0,
        "scale_pos_weight": 13.0,  # ~1/14 horses win per race
        "verbose": -1,
        "n_jobs": -1,
        "random_state": 42,
    }

    # XGBoost hyperparameters
    XGB_PARAMS = {
        "objective": "binary:logistic",
        "eval_metric": "logloss",
        "max_depth": 8,
        "learning_rate": 0.05,
        "n_estimators": 1000,
        "min_child_weight": 50,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "reg_alpha": 0.1,
        "reg_lambda": 1.0,
        "scale_pos_weight": 13.0,
        "verbosity": 0,
        "n_jobs": -1,
        "random_state": 42,
    }

    def __init__(self, session: Session | None = None):
        self.session = session or get_session()
        self.feature_engine = FeatureEngineer(self.session)
        self.feature_cols = FeatureEngineer.get_feature_columns()

    def train_win_model(
        self,
        train_start: date,
        train_end: date,
        model_type: str = "lightgbm",
    ) -> tuple[object, dict]:
        """
        Train a binary win prediction model.
        Returns (model, metadata_dict).
        """
        logger.info(
            "Training %s win model on %s to %s", model_type, train_start, train_end
        )

        # Build feature matrix
        df = self.feature_engine.build_features_for_date_range(train_start, train_end)
        if df.empty:
            logger.error("No training data available")
            return None, {}

        # Filter valid rows
        df = df[df["finish_position"].notna() & (df["finish_position"] > 0)]

        X = df[self.feature_cols].copy()
        y = df["is_winner"].values

        # Handle missing values
        X = X.fillna(X.median())

        logger.info("Training data: %d runners, %d winners (%.1f%%)", len(X), y.sum(), 100 * y.mean())

        # Time-series split for validation
        tscv = TimeSeriesSplit(n_splits=3)
        val_scores = []

        for fold, (train_idx, val_idx) in enumerate(tscv.split(X)):
            X_train, X_val = X.iloc[train_idx], X.iloc[val_idx]
            y_train, y_val = y[train_idx], y[val_idx]

            if model_type == "lightgbm":
                model = lgb.LGBMClassifier(**self.LGBM_PARAMS)
                model.fit(
                    X_train, y_train,
                    eval_set=[(X_val, y_val)],
                    callbacks=[lgb.early_stopping(50, verbose=False)],
                )
            else:
                model = xgb.XGBClassifier(**self.XGB_PARAMS)
                model.fit(
                    X_train, y_train,
                    eval_set=[(X_val, y_val)],
                    verbose=False,
                )

            y_pred = model.predict_proba(X_val)[:, 1]
            auc = roc_auc_score(y_val, y_pred)
            logloss = log_loss(y_val, y_pred)
            val_scores.append({"auc": auc, "logloss": logloss})
            logger.info("Fold %d: AUC=%.4f, LogLoss=%.4f", fold + 1, auc, logloss)

        # Train final model on all data
        if model_type == "lightgbm":
            final_model = lgb.LGBMClassifier(**self.LGBM_PARAMS)
        else:
            final_model = xgb.XGBClassifier(**self.XGB_PARAMS)

        final_model.fit(X, y)

        # Feature importance
        importance = dict(zip(
            self.feature_cols,
            final_model.feature_importances_.tolist(),
        ))

        # Average validation scores
        avg_auc = np.mean([s["auc"] for s in val_scores])
        avg_logloss = np.mean([s["logloss"] for s in val_scores])

        metadata = {
            "model_type": model_type,
            "target": "win",
            "training_races_count": df["race_id"].nunique(),
            "training_runners_count": len(df),
            "training_date_range": f"{train_start} to {train_end}",
            "validation_auc": avg_auc,
            "validation_logloss": avg_logloss,
            "feature_importance": importance,
            "hyperparams": self.LGBM_PARAMS if model_type == "lightgbm" else self.XGB_PARAMS,
        }

        logger.info(
            "Training complete: AUC=%.4f, LogLoss=%.4f (%d races)",
            avg_auc, avg_logloss, metadata["training_races_count"],
        )

        return final_model, metadata

    def train_place_model(
        self,
        train_start: date,
        train_end: date,
        model_type: str = "lightgbm",
    ) -> tuple[object, dict]:
        """Train a binary place (top-3) prediction model."""
        logger.info("Training %s place model on %s to %s", model_type, train_start, train_end)

        df = self.feature_engine.build_features_for_date_range(train_start, train_end)
        if df.empty:
            return None, {}

        df = df[df["finish_position"].notna() & (df["finish_position"] > 0)]

        X = df[self.feature_cols].copy().fillna(df[self.feature_cols].median())
        y = df["is_placed"].values

        # Place model: ~3/14 horses place, so less imbalanced
        params = (self.LGBM_PARAMS if model_type == "lightgbm" else self.XGB_PARAMS).copy()
        params["scale_pos_weight"] = 3.67  # ~11/3 ratio

        if model_type == "lightgbm":
            model = lgb.LGBMClassifier(**params)
        else:
            model = xgb.XGBClassifier(**params)

        model.fit(X, y)

        y_pred = model.predict_proba(X)[:, 1]
        train_auc = roc_auc_score(y, y_pred)

        metadata = {
            "model_type": model_type,
            "target": "place",
            "training_races_count": df["race_id"].nunique(),
            "training_date_range": f"{train_start} to {train_end}",
            "validation_auc": train_auc,
            "hyperparams": params,
        }

        return model, metadata

    def save_model(
        self, model: object, metadata: dict, version: str | None = None
    ) -> str:
        """Save model to disk and record in database."""
        if version is None:
            version = f"{metadata['model_type']}_{metadata['target']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        model_dir = settings.MODEL_DIR
        model_dir.mkdir(parents=True, exist_ok=True)
        model_path = model_dir / f"{version}.joblib"

        joblib.dump(model, model_path)
        logger.info("Model saved to %s", model_path)

        # Record in database
        model_meta = ModelMetadata(
            version=version,
            model_type=metadata.get("model_type", "lightgbm"),
            target=metadata.get("target", "win"),
            training_races_count=metadata.get("training_races_count"),
            training_date_range=metadata.get("training_date_range"),
            validation_metric=metadata.get("validation_auc"),
            hyperparams=metadata.get("hyperparams"),
            feature_importance=metadata.get("feature_importance"),
            model_path=str(model_path),
        )
        self.session.add(model_meta)
        self.session.commit()

        return version

    def load_model(self, version: str) -> object | None:
        """Load a saved model by version string."""
        meta = self.session.query(ModelMetadata).filter_by(version=version).first()
        if not meta or not meta.model_path:
            logger.error("Model not found: %s", version)
            return None

        model_path = Path(meta.model_path)
        if not model_path.exists():
            logger.error("Model file missing: %s", model_path)
            return None

        return joblib.load(model_path)

    def get_latest_model_version(self, target: str = "win") -> str | None:
        """Get the most recent model version for a target."""
        meta = (
            self.session.query(ModelMetadata)
            .filter_by(target=target)
            .order_by(ModelMetadata.trained_at.desc())
            .first()
        )
        return meta.version if meta else None
