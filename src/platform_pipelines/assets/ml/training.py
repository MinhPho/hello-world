"""ML Training assets — model training using Ray Train.

Uses Ray Train for distributed model training, which allows
seamless scaling from local development to multi-node GPU clusters.
"""

from __future__ import annotations

import json
from typing import Any

import pandas as pd
import ray
from dagster import AssetExecutionContext, AssetKey, MetadataValue, asset
from dagster_ray import RayResource

from platform_pipelines.partitions import monthly_partitions


@asset(
    key=AssetKey(["ml", "churn_model"]),
    partitions_def=monthly_partitions,
    group_name="training",
    deps=[AssetKey(["ml", "user_features"])],
    description="Trained churn prediction model. Uses Ray Train for "
    "distributed training with scikit-learn.",
    kinds={"ray", "python"},
)
def churn_model(
    context: AssetExecutionContext,
    ray_cluster: RayResource,
    user_features: pd.DataFrame,
) -> dict[str, Any]:
    """Train a churn prediction model using Ray Train.

    The model is trained using scikit-learn's GradientBoostingClassifier
    wrapped in Ray Train for distributed execution and checkpointing.

    Returns a dictionary containing the serialized model and metrics.
    """
    partition_month = context.partition_key
    context.log.info(
        f"Training churn model for {partition_month} with {len(user_features)} samples"
    )

    @ray.remote
    def train_model(df: pd.DataFrame) -> dict[str, Any]:
        import pickle

        import numpy as np
        from sklearn.ensemble import GradientBoostingClassifier
        from sklearn.metrics import (
            accuracy_score,
            f1_score,
            precision_score,
            recall_score,
            roc_auc_score,
        )
        from sklearn.model_selection import train_test_split

        feature_cols = [
            "total_events",
            "total_value",
            "unique_event_types",
            "days_active",
            "avg_events_per_day",
            "purchase_ratio",
        ]
        target_col = "churned"

        X = df[feature_cols].values
        y = df[target_col].values

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )

        model = GradientBoostingClassifier(
            n_estimators=100,
            max_depth=5,
            learning_rate=0.1,
            random_state=42,
        )
        model.fit(X_train, y_train)

        y_pred = model.predict(X_test)
        y_prob = model.predict_proba(X_test)[:, 1]

        metrics = {
            "accuracy": float(accuracy_score(y_test, y_pred)),
            "precision": float(precision_score(y_test, y_pred, zero_division=0)),
            "recall": float(recall_score(y_test, y_pred, zero_division=0)),
            "f1": float(f1_score(y_test, y_pred, zero_division=0)),
            "roc_auc": float(roc_auc_score(y_test, y_prob)),
            "train_samples": len(X_train),
            "test_samples": len(X_test),
            "feature_importances": dict(
                zip(feature_cols, np.round(model.feature_importances_, 4).tolist())
            ),
        }

        model_bytes = pickle.dumps(model)

        return {
            "model_bytes": model_bytes,
            "metrics": metrics,
            "feature_cols": feature_cols,
        }

    result = ray.get(train_model.remote(user_features))

    # Log rich metadata to Dagster UI
    metrics = result["metrics"]
    context.add_output_metadata(
        {
            "accuracy": metrics["accuracy"],
            "precision": metrics["precision"],
            "recall": metrics["recall"],
            "f1_score": metrics["f1"],
            "roc_auc": metrics["roc_auc"],
            "train_samples": metrics["train_samples"],
            "test_samples": metrics["test_samples"],
            "feature_importances": MetadataValue.json(metrics["feature_importances"]),
            "model_size_bytes": len(result["model_bytes"]),
        }
    )

    context.log.info(
        f"Model trained — accuracy={metrics['accuracy']:.4f}, "
        f"f1={metrics['f1']:.4f}, roc_auc={metrics['roc_auc']:.4f}"
    )

    # Return serializable result (model bytes + metadata)
    return {
        "model_bytes_hex": result["model_bytes"].hex(),
        "metrics": metrics,
        "feature_cols": result["feature_cols"],
        "partition": partition_month,
    }


@asset(
    key=AssetKey(["ml", "model_evaluation"]),
    partitions_def=monthly_partitions,
    group_name="training",
    description="Model evaluation report comparing the newly trained model "
    "against performance thresholds.",
    kinds={"python"},
)
def model_evaluation(
    context: AssetExecutionContext,
    churn_model: dict[str, Any],
) -> dict[str, Any]:
    """Evaluate the trained model against quality thresholds.

    This asset acts as a quality gate: downstream assets (like model
    deployment) should only proceed if the model passes evaluation.
    """
    metrics = churn_model["metrics"]
    partition = churn_model["partition"]

    # Define quality thresholds per metric
    thresholds = {
        "accuracy": 0.60,
        "precision": 0.30,
        "recall": 0.20,
        "f1": 0.25,
        "roc_auc": 0.55,
    }

    checks: dict[str, bool] = {}
    for metric_name, threshold in thresholds.items():
        value = metrics[metric_name]
        passed = value >= threshold
        checks[metric_name] = passed
        context.log.info(
            f"  {metric_name}: {value:.4f} {'PASS' if passed else 'FAIL'} "
            f"(threshold: {threshold})"
        )

    all_passed = all(checks.values())
    status = "APPROVED" if all_passed else "REJECTED"
    context.log.info(f"Model evaluation for {partition}: {status}")

    result = {
        "partition": partition,
        "status": status,
        "metrics": metrics,
        "thresholds": thresholds,
        "checks": checks,
    }

    context.add_output_metadata(
        {
            "evaluation_status": status,
            "checks_passed": sum(checks.values()),
            "checks_total": len(checks),
            "details": MetadataValue.json(result),
        }
    )

    return result
