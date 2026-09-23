"""TokenRelay v4 — A/B Testing Framework.

Provides Experiment, ABTestRunner, StatisticalTest, MetricsTracker,
and ExperimentRegistry for comparing strategies with statistical rigor.
"""
import uuid
import time
import math
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Optional, List, Dict, Any
from collections import defaultdict


class ExperimentStatus(Enum):
    PLANNED = "planned"
    RUNNING = "running"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


class MetricType(Enum):
    TOKEN_SAVINGS = "token_savings"
    LATENCY = "latency"
    SUCCESS_RATE = "success_rate"
    THROUGHPUT = "throughput"
    ERROR_RATE = "error_rate"


@dataclass
class Experiment:
    id: str
    name: str
    hypothesis: str
    control_variant: str
    treatment_variant: str
    metrics: List[str] = field(default_factory=list)
    status: str = ExperimentStatus.PLANNED.value
    created_at: float = field(default_factory=time.time)
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    config: Dict[str, Any] = field(default_factory=dict)
    comparison_type: str = "swarm_vs_direct"

    def __post_init__(self):
        if not self.id:
            self.id = str(uuid.uuid4())
        if not isinstance(self.metrics, list):
            self.metrics = list(self.metrics) if self.metrics else ["token_savings", "latency", "success_rate"]


@dataclass
class MetricsTracker:
    """Tracks per-variant metrics for an A/B experiment."""
    variant: str = ""
    token_savings: float = 0.0
    latency_ms: float = 0.0
    success_rate: float = 0.0
    throughput: float = 0.0
    error_rate: float = 0.0
    samples: int = 0
    total_tokens_used: int = 0
    total_requests: int = 0
    total_token_savings: float = 0.0
    _raw_latencies: List[float] = field(default_factory=list)
    _raw_token_savings: List[float] = field(default_factory=list)

    def record(self, token_savings: float, latency_ms: float, success: bool):
        """Record a single observation."""
        self._raw_latencies.append(latency_ms)
        self._raw_token_savings.append(token_savings)
        self.samples += 1
        self.total_token_savings += token_savings
        self.latency_ms = sum(self._raw_latencies) / len(self._raw_latencies)
        if success:
            self.success_rate = sum(1 for _ in self._raw_latencies[-self.samples:]) / max(self.samples, 1)
            self.success_rate = (self.samples - len([l for l in self._raw_latencies if l <= 0])) / max(self.samples, 1) if self.samples else 0
        self.total_requests += 1
        self.token_savings = self.total_token_savings / max(self.samples, 1)

    def get_summary(self) -> Dict[str, Any]:
        return {
            "variant": self.variant,
            "samples": self.samples,
            "token_savings": round(self.token_savings, 4),
            "latency_ms": round(self.latency_ms, 2),
            "success_rate": round(self.success_rate, 4),
            "throughput": round(self.throughput, 2),
            "error_rate": round(self.error_rate, 4),
            "total_tokens_used": self.total_tokens_used,
        }


class StatisticalTest:
    """Statistical tests for A/B experiment analysis."""

    @staticmethod
    def mean(values: List[float]) -> float:
        return sum(values) / max(len(values), 1)

    @staticmethod
    def variance(values: List[float]) -> float:
        if len(values) < 2:
            return 0.0
        m = StatisticalTest.mean(values)
        return sum((v - m) ** 2 for v in values) / (len(values) - 1)

    @staticmethod
    def std_dev(values: List[float]) -> float:
        return math.sqrt(StatisticalTest.variance(values))

    @staticmethod
    def calculate_p_value(control: List[float], treatment: List[float]) -> float:
        """Two-sample t-test approximation for p-value."""
        if len(control) < 2 or len(treatment) < 2:
            return 1.0
        m1, m2 = StatisticalTest.mean(control), StatisticalTest.mean(treatment)
        v1, v2 = StatisticalTest.variance(control), StatisticalTest.variance(treatment)
        n1, n2 = len(control), len(treatment)
        pooled_se = math.sqrt(v1 / n1 + v2 / n2)
        if pooled_se == 0:
            return 1.0 if m1 == m2 else 0.0
        t_stat = (m2 - m1) / pooled_se
        # Approximate p-value using normal distribution for large samples
        # Using the error function approximation
        return StatisticalTest._t_to_p(abs(t_stat), n1 + n2 - 2)

    @staticmethod
    def _t_to_p(t: float, df: int) -> float:
        """Convert t-statistic to p-value using approximation."""
        if df < 1:
            return 1.0
        x = df / (df + t * t)
        p = StatisticalTest._betainc(df / 2.0, 0.5, x)
        return p

    @staticmethod
    def _betainc(a: float, b: float, x: float) -> float:
        """Incomplete beta function approximation."""
        if x <= 0:
            return 0.0
        if x >= 1:
            return 1.0
        # Simple approximation using regularized incomplete beta
        # Using the relationship with the Student's t-distribution
        z = a * math.log(x) + b * math.log(1.0 - x) - math.lgamma(a + b) + math.lgamma(a) + math.lgamma(b)
        return math.exp(z) / (a * x) if x > 0 else 0.5

    @classmethod
    def confidence_interval(cls, values: List[float], confidence: float = 0.95) -> tuple:
        """Calculate confidence interval for a list of values."""
        if len(values) < 2:
            m = cls.mean(values) if values else 0.0
            return (m, m)
        m = cls.mean(values)
        se = cls.std_dev(values) / math.sqrt(len(values))
        z = cls._z_score(confidence)
        return (m - z * se, m + z * se)

    @staticmethod
    def _z_score(confidence: float) -> float:
        """Approximate z-score for confidence level."""
        # Simplified: 95% -> 1.96, 99% -> 2.576
        if confidence >= 0.99:
            return 2.576
        elif confidence >= 0.95:
            return 1.96
        elif confidence >= 0.90:
            return 1.645
        return 1.0

    @classmethod
    def is_significant(cls, control: List[float], treatment: List[float], alpha: float = 0.05) -> bool:
        """Check if treatment is significantly different from control."""
        p = cls.calculate_p_value(control, treatment)
        return p < alpha


class ABTestRunner:
    """Run A/B experiments comparing two strategies."""

    SUPPORTED_COMPARISONS = {
        "swarm_vs_direct",
        "agent_count_6_vs_8",
        "compression_level_50_vs_90",
    }

    def __init__(self):
        self._experiments: Dict[str, Experiment] = {}
        self._metrics: Dict[str, Dict[str, MetricsTracker]] = defaultdict(lambda: {})
        self._results: Dict[str, Dict[str, Any]] = {}

    def create_experiment(
        self,
        name: str,
        hypothesis: str,
        control_variant: str,
        treatment_variant: str,
        comparison_type: str = "swarm_vs_direct",
        metrics: Optional[List[str]] = None,
        config: Optional[Dict[str, Any]] = None,
    ) -> Experiment:
        """Create and register a new experiment."""
        if comparison_type not in self.SUPPORTED_COMPARISONS:
            raise ValueError(f"Unsupported comparison type: {comparison_type}. Must be one of {self.SUPPORTED_COMPARISONS}")

        exp = Experiment(
            id=str(uuid.uuid4()),
            name=name,
            hypothesis=hypothesis,
            control_variant=control_variant,
            treatment_variant=treatment_variant,
            comparison_type=comparison_type,
            metrics=metrics or ["token_savings", "latency", "success_rate"],
            config=config or {},
        )
        self._experiments[exp.id] = exp
        exp.status = ExperimentStatus.RUNNING.value
        exp.started_at = time.time()
        return exp

    def record_result(self, experiment_id: str, variant: str, token_savings: float, latency_ms: float, success: bool):
        """Record a result observation for an experiment variant."""
        exp = self._experiments.get(experiment_id)
        if not exp:
            raise ValueError(f"Experiment {experiment_id} not found")
        if variant not in (exp.control_variant, exp.treatment_variant):
            raise ValueError(f"Variant {variant} not part of experiment {experiment_id}")

        if variant not in self._metrics[experiment_id]:
            self._metrics[experiment_id][variant] = MetricsTracker(variant=variant)

        tracker = self._metrics[experiment_id][variant]
        tracker.record(token_savings, latency_ms, success)

    def get_experiment(self, experiment_id: str) -> Optional[Experiment]:
        """Get experiment by ID."""
        return self._experiments.get(experiment_id)

    def list_experiments(self, status: Optional[str] = None) -> List[Experiment]:
        """List all experiments, optionally filtered by status."""
        exps = list(self._experiments.values())
        if status:
            exps = [e for e in exps if e.status == status]
        return exps

    def complete_experiment(self, experiment_id: str) -> Dict[str, Any]:
        """Complete an experiment and compute statistical results."""
        exp = self._experiments.get(experiment_id)
        if not exp:
            raise ValueError(f"Experiment {experiment_id} not found")

        exp.status = ExperimentStatus.COMPLETED.value
        exp.completed_at = time.time()

        control_tracker = self._metrics[experiment_id].get(exp.control_variant)
        treatment_tracker = self._metrics[experiment_id].get(exp.treatment_variant)

        result = {
            "experiment_id": experiment_id,
            "name": exp.name,
            "status": exp.status,
            "comparison_type": exp.comparison_type,
            "control_variant": exp.control_variant,
            "treatment_variant": exp.treatment_variant,
        }

        if control_tracker and treatment_tracker:
            control_values = [control_tracker.latency_ms] * max(control_tracker.samples, 1)
            treatment_values = [treatment_tracker.latency_ms] * max(treatment_tracker.samples, 1)
            result["control_summary"] = control_tracker.get_summary()
            result["treatment_summary"] = treatment_tracker.get_summary()
            result["p_value"] = StatisticalTest.calculate_p_value(
                [control_tracker.latency_ms] * max(control_tracker.samples, 1),
                [treatment_tracker.latency_ms] * max(treatment_tracker.samples, 1)
            )
            result["significant"] = StatisticalTest.is_significant(
                [control_tracker.latency_ms] * max(control_tracker.samples, 1),
                [treatment_tracker.latency_ms] * max(treatment_tracker.samples, 1)
            )
            result["confidence_interval"] = StatisticalTest.confidence_interval(
                [control_tracker.latency_ms] * max(control_tracker.samples, 1) + [treatment_tracker.latency_ms] * max(treatment_tracker.samples, 1)
            )
        else:
            result["significant"] = False
            result["p_value"] = 1.0

        self._results[experiment_id] = result
        return result

    def run_experiment(
        self,
        name: str,
        hypothesis: str,
        comparison_type: str,
        control_variant: str,
        treatment_variant: str,
        control_data: List[float],
        treatment_data: List[float],
    ) -> Dict[str, Any]:
        """Create and run a complete experiment with pre-collected data."""
        exp = self.create_experiment(name, hypothesis, control_variant, treatment_variant, comparison_type)
        for val in control_data:
            self.record_result(exp.id, control_variant, val, val, True)
        for val in treatment_data:
            self.record_result(exp.id, treatment_variant, val, val, True)
        return self.complete_experiment(exp.id)


class ExperimentRegistry:
    """Registry for listing and retrieving experiments."""

    def __init__(self, runner: Optional[ABTestRunner] = None):
        self._runner = runner or ABTestRunner()

    def list_experiments(self, status: Optional[str] = None) -> List[Experiment]:
        """List all experiments, optionally filtered by status."""
        exps = list(self._runner._experiments.values())
        if status:
            exps = [e for e in exps if e.status == status]
        return exps

    def get_experiment(self, experiment_id: str) -> Optional[Experiment]:
        """Get a single experiment by ID."""
        return self._runner.get_experiment(experiment_id)

    def get_results(self, experiment_id: str) -> Optional[Dict[str, Any]]:
        """Get completed results for an experiment."""
        return self._runner._results.get(experiment_id)

    def create_experiment(
        self,
        name: str,
        hypothesis: str,
        control_variant: str,
        treatment_variant: str,
        comparison_type: str = "swarm_vs_direct",
        metrics: Optional[List[str]] = None,
        config: Optional[Dict[str, Any]] = None,
    ) -> Experiment:
        """Create and register a new experiment."""
        return self._runner.create_experiment(name, hypothesis, control_variant, treatment_variant, comparison_type, metrics, config)

    def record_result(self, experiment_id: str, variant: str, token_savings: float, latency_ms: float, success: bool):
        """Record a result observation."""
        self._runner.record_result(experiment_id, variant, token_savings, latency_ms, success)

    def complete_experiment(self, experiment_id: str) -> Dict[str, Any]:
        """Complete an experiment."""
        return self._runner.complete_experiment(experiment_id)
