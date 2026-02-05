"""External Ray job for distributed ML training.

Submitted via Dagster Pipes for workloads requiring dedicated GPU
resources or multi-node distributed training with Ray Train.

Can be tested locally:
    python ray_jobs/ml_training_job.py
"""

from __future__ import annotations

import ray


def run_training_job() -> dict:
    """Execute distributed model training on Ray."""
    if not ray.is_initialized():
        ray.init()

    print(f"Ray cluster resources: {ray.cluster_resources()}")

    # Example: distributed training using Ray remote functions
    # In production, use ray.train with your framework of choice
    # (PyTorch, TensorFlow, XGBoost, LightGBM, etc.)

    @ray.remote
    def train_fold(fold_id: int, num_samples: int = 10_000) -> dict:
        """Train a model on a single fold (simulated)."""
        import numpy as np
        from sklearn.ensemble import RandomForestClassifier
        from sklearn.metrics import accuracy_score

        rng = np.random.default_rng(fold_id)
        X = rng.standard_normal((num_samples, 10))
        y = (X[:, 0] + X[:, 1] > 0).astype(int)

        split = int(0.8 * num_samples)
        model = RandomForestClassifier(n_estimators=50, random_state=fold_id)
        model.fit(X[:split], y[:split])
        accuracy = accuracy_score(y[split:], model.predict(X[split:]))

        return {"fold_id": fold_id, "accuracy": float(accuracy)}

    # Distributed cross-validation: train 5 folds in parallel on Ray
    futures = [train_fold.remote(fold_id=i) for i in range(5)]
    fold_results = ray.get(futures)

    accuracies = [r["accuracy"] for r in fold_results]
    mean_accuracy = sum(accuracies) / len(accuracies)

    result = {
        "fold_results": fold_results,
        "mean_accuracy": float(mean_accuracy),
        "num_folds": len(fold_results),
        "status": "completed",
    }

    print(f"Cross-validation complete — mean accuracy: {mean_accuracy:.4f}")
    return result


if __name__ == "__main__":
    try:
        from dagster_pipes import open_dagster_pipes

        with open_dagster_pipes() as context:
            result = run_training_job()
            context.report_asset_materialization(metadata=result)
    except ImportError:
        result = run_training_job()
        print(f"Result: {result}")
