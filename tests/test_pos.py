"""Tests for TokenRelay v3 Predictive Output Streaming (POS)."""

import sys
import time
import threading
from pathlib import Path
from dataclasses import asdict

SRC_DIR = Path(__file__).resolve().parent.parent / "src"
sys.path.insert(0, str(SRC_DIR))

from pos import (
    POSConfig, SectionPrediction, PredictedStructure, Chunk, ChunkMetrics,
    Phase, PredictionConfidence, ChunkPriority, PredictorMode,
    ResponsePredictor, ProgressiveRenderer, ChunkManager,
    BackpressureController, PredictionCache,
    POSPredictionError, POSRenderingError, POSBackpressureError,
    create_pos_system, stream_response,
)

# Zero-delay config for all tests — no time.sleep() calls
ZERO_CONFIG = POSConfig(
    skeleton_phase_delay_ms=0,
    detail_phase_delay_ms=0,
    conclusion_phase_delay_ms=0,
)

# ─────────────────────────────────────────────
# POSConfig Tests
# ─────────────────────────────────────────────

def test_pos_config_defaults():
    config = POSConfig()
    assert config.prediction_confidence_threshold == 0.5
    assert config.progressive_rendering_enabled is True
    assert config.default_chunk_size == 100
    assert config.max_chunk_size == 500
    assert config.min_chunk_size == 10
    assert config.backpressure_window_size == 100
    assert config.high_watermark == 0.8
    assert config.low_watermark == 0.3
    assert config.prediction_cache_size == 1000
    print("✓ test_pos_config_defaults")

def test_pos_config_custom():
    config = POSConfig(default_chunk_size=200, max_chunk_size=1000, high_watermark=0.9)
    assert config.default_chunk_size == 200
    assert config.max_chunk_size == 1000
    assert config.high_watermark == 0.9
    print("✓ test_pos_config_custom")

def test_pos_config_as_dict():
    config = POSConfig()
    d = asdict(config)
    assert "default_chunk_size" in d and d["default_chunk_size"] == 100
    assert "high_watermark" in d
    print("✓ test_pos_config_as_dict")

# ─────────────────────────────────────────────
# Enum Tests
# ─────────────────────────────────────────────

def test_phase_enum_values():
    assert Phase.SKELETON.value == "skeleton"
    assert Phase.DETAILS.value == "details"
    assert Phase.CONCLUSION.value == "conclusion"
    print("✓ test_phase_enum_values")

def test_prediction_confidence_enum():
    assert PredictionConfidence.LOW.value == "low"
    assert PredictionConfidence.MEDIUM.value == "medium"
    assert PredictionConfidence.HIGH.value == "high"
    print("✓ test_prediction_confidence_enum")

def test_chunk_priority_enum():
    assert ChunkPriority.SKELETON.value == 0
    assert ChunkPriority.DETAIL.value == 1
    assert ChunkPriority.CONCLUSION.value == 2
    print("✓ test_chunk_priority_enum")

def test_predictor_mode_enum():
    assert PredictorMode.QUERY_PATTERN.value == "query_pattern"
    assert PredictorMode.TEMPLATE_MATCH.value == "template_match"
    assert PredictorMode.HYBRID.value == "hybrid"
    print("✓ test_predictor_mode_enum")

# ─────────────────────────────────────────────
# Data Class Tests
# ─────────────────────────────────────────────

def test_section_prediction():
    sec = SectionPrediction(section_name="intro", section_type="introduction",
                            estimated_tokens=50, order=1, confidence=0.9)
    assert sec.section_name == "intro" and sec.confidence == 0.9
    print("✓ test_section_prediction")

def test_predicted_structure():
    sections = [SectionPrediction("intro", "introduction", 50, 1, confidence=0.9)]
    structure = PredictedStructure(skeleton="Test", sections=sections, conclusion="End", confidence=0.9)
    assert structure.skeleton == "Test" and len(structure.sections) == 1 and structure.confidence == 0.9
    print("✓ test_predicted_structure")

def test_predicted_structure_to_dict():
    sections = [SectionPrediction("s", "type", 10, 1)]
    structure = PredictedStructure(skeleton="s", sections=sections, conclusion="c", confidence=0.5)
    d = asdict(structure)
    assert d["skeleton"] == "s" and d["confidence"] == 0.5
    print("✓ test_predicted_structure_to_dict")

def test_chunk_dataclass():
    chunk = Chunk(content="Hello", phase=Phase.SKELETON, chunk_index=0, total_chunks_in_phase=3, priority=0, token_count=5)
    assert chunk.content == "Hello" and chunk.phase == Phase.SKELETON
    d = chunk.to_dict()
    assert d["content"] == "Hello" and d["phase"] == "skeleton"
    print("✓ test_chunk_dataclass")

def test_chunk_metrics():
    metrics = ChunkMetrics(chunks_sent=10, total_tokens_sent=1000, avg_chunk_size=100.0)
    assert metrics.chunks_sent == 10 and metrics.total_tokens_sent == 1000
    print("✓ test_chunk_metrics")

# ─────────────────────────────────────────────
# PredictionCache Tests
# ─────────────────────────────────────────────

def test_prediction_cache_put_get():
    cache = PredictionCache(max_size=5)
    s = PredictedStructure(skeleton="test", sections=[], conclusion="end", confidence=0.5)
    cache.put("h1", s)
    assert cache.get("h1") is not None and cache.get("h1").skeleton == "test"
    print("✓ test_prediction_cache_put_get")

def test_prediction_cache_miss():
    cache = PredictionCache(max_size=5)
    assert cache.get("nonexistent") is None
    print("✓ test_prediction_cache_miss")

def test_prediction_cache_lru_eviction():
    cache = PredictionCache(max_size=3)
    for i in range(5):
        s = PredictedStructure(skeleton=f"s{i}", sections=[], conclusion="c", confidence=0.5)
        cache.put(f"h{i}", s)
    assert cache.size == 3
    assert cache.get("h0") is None
    assert cache.get("h1") is None
    print("✓ test_prediction_cache_lru_eviction")

def test_prediction_cache_invalidate():
    cache = PredictionCache(max_size=5)
    s = PredictedStructure(skeleton="test", sections=[], conclusion="end", confidence=0.5)
    cache.put("h1", s)
    cache.invalidate("h1")
    assert cache.get("h1") is None
    print("✓ test_prediction_cache_invalidate")

def test_prediction_cache_clear():
    cache = PredictionCache(max_size=10)
    for i in range(5):
        s = PredictedStructure(skeleton=f"s{i}", sections=[], conclusion="c", confidence=0.5)
        cache.put(f"h{i}", s)
    assert cache.size == 5
    cache.clear()
    assert cache.size == 0
    print("✓ test_prediction_cache_clear")

# ─────────────────────────────────────────────
# ResponsePredictor Tests
# ─────────────────────────────────────────────

def test_predictor_predict_scientific():
    predictor = ResponsePredictor()
    structure = predictor.predict("How does photosynthesis work?")
    assert structure.skeleton != "" and len(structure.sections) > 0 and structure.confidence > 0.5
    print("✓ test_predictor_predict_scientific")

def test_predictor_predict_coding():
    predictor = ResponsePredictor()
    structure = predictor.predict("Write a Python function to sort a list")
    assert structure.skeleton != "" and len(structure.sections) > 0
    print("✓ test_predictor_predict_coding")

def test_predictor_predict_comparison():
    predictor = ResponsePredictor()
    structure = predictor.predict("Compare Python and JavaScript")
    assert structure.skeleton != "" and len(structure.sections) >= 2
    print("✓ test_predictor_predict_comparison")

def test_predictor_predict_definition():
    predictor = ResponsePredictor()
    structure = predictor.predict("What is machine learning?")
    assert structure.skeleton != "" and len(structure.sections) > 0
    print("✓ test_predictor_predict_definition")

def test_predictor_predict_list():
    predictor = ResponsePredictor()
    structure = predictor.predict("List the steps to deploy a web app")
    assert structure.skeleton != "" and len(structure.sections) > 0
    print("✓ test_predictor_predict_list")

def test_predictor_predict_debug():
    predictor = ResponsePredictor()
    structure = predictor.predict("Debug why my code doesn't work")
    assert structure.skeleton != "" and len(structure.sections) > 0
    print("✓ test_predictor_predict_debug")

def test_predictor_predict_empty_raises():
    predictor = ResponsePredictor()
    try:
        predictor.predict("")
        assert False
    except ValueError:
        pass
    try:
        predictor.predict("   ")
        assert False
    except ValueError:
        pass
    print("✓ test_predictor_predict_empty_raises")

def test_predictor_predict_non_string_raises():
    predictor = ResponsePredictor()
    try:
        predictor.predict(123)
        assert False
    except ValueError:
        pass
    print("✓ test_predictor_predict_non_string_raises")

def test_predictor_caching():
    predictor = ResponsePredictor()
    s1 = predictor.predict("How does photosynthesis work?")
    s2 = predictor.predict("How does photosynthesis work?")
    assert s1.query_hash == s2.query_hash and s1 is s2
    stats = predictor.get_prediction_stats()
    assert stats["cache_hits"] == 1 and stats["total_predictions"] == 1
    print("✓ test_predictor_caching")

def test_predictor_cache_miss_increments():
    predictor = ResponsePredictor()
    predictor.predict("Query A")
    predictor.predict("Query B")
    stats = predictor.get_prediction_stats()
    assert stats["total_predictions"] == 2 and stats["cache_hits"] == 0
    print("✓ test_predictor_cache_miss_increments")

def test_predictor_with_confidence():
    predictor = ResponsePredictor()
    structure, confidence = predictor.predict_with_confidence("How does photosynthesis work?")
    assert isinstance(structure, PredictedStructure) and isinstance(confidence, float) and 0.0 <= confidence <= 1.0
    print("✓ test_predictor_with_confidence")

def test_predictor_get_stats():
    predictor = ResponsePredictor()
    predictor.predict("Test query")
    stats = predictor.get_prediction_stats()
    assert "total_predictions" in stats and "cache_hits" in stats and "cache_misses" in stats
    print("✓ test_predictor_get_stats")

def test_predictor_reset_stats():
    predictor = ResponsePredictor()
    predictor.predict("Test")
    predictor.reset_stats()
    stats = predictor.get_prediction_stats()
    assert stats["total_predictions"] == 0
    print("✓ test_predictor_reset_stats")

def test_predictor_mode_setting():
    predictor = ResponsePredictor()
    assert predictor.mode == PredictorMode.HYBRID
    predictor.mode = PredictorMode.QUERY_PATTERN
    assert predictor.mode == PredictorMode.QUERY_PATTERN
    print("✓ test_predictor_mode_setting")

def test_predictor_unknown_query():
    predictor = ResponsePredictor()
    structure = predictor.predict("asdf jkl zxc vbnm qwr tyu io p asdf jkl zxc vbnm qwr")
    assert structure.skeleton != "" and structure.confidence > 0.0 and len(structure.sections) > 0
    assert len(structure.sections) > 0
    print("✓ test_predictor_unknown_query")

# ─────────────────────────────────────────────
# BackpressureController Tests
# ─────────────────────────────────────────────

def test_backpressure_initial_state():
    bp = BackpressureController()
    state = bp.get_backpressure_state()
    assert state["utilization"] == 0.0 and state["total_sent"] == 0 and state["total_dropped"] == 0
    print("✓ test_backpressure_initial_state")

def test_backpressure_record_chunk():
    bp = BackpressureController()
    bp.record_chunk_sent(100)
    assert bp.total_sent == 100
    print("✓ test_backpressure_record_chunk")

def test_backpressure_record_drop():
    bp = BackpressureController()
    bp.record_chunk_sent(100, dropped=True)
    assert bp.total_dropped >= 1
    print("✓ test_backpressure_record_drop")

def test_backpressure_utilization():
    bp = BackpressureController(config=POSConfig(backpressure_window_size=10))
    for _ in range(8):
        bp.record_chunk_sent(10)
    assert bp.get_utilization() > 0.0
    print("✓ test_backpressure_utilization")

def test_backpressure_rate_adjustment():
    bp = BackpressureController(config=POSConfig(backpressure_window_size=5))
    for _ in range(5):
        bp.record_chunk_sent(100)
    assert bp.get_adjusted_rate() < 1.0
    print("✓ test_backpressure_rate_adjustment")

def test_backpressure_adjust_chunk_size():
    bp = BackpressureController()
    size = bp.adjust_chunk_size()
    assert 10 <= size <= 500
    print("✓ test_backpressure_adjust_chunk_size")

def test_backpressure_get_current_rate():
    bp = BackpressureController(config=POSConfig(backpressure_window_size=5))
    bp.record_chunk_sent(100)
    assert bp.get_current_rate() >= 0.0
    print("✓ test_backpressure_get_current_rate")

def test_backpressure_callbacks():
    bp = BackpressureController()
    calls = []
    bp.register_callback(lambda state: calls.append(state))
    bp.record_chunk_sent(100)
    assert len(bp._callbacks) == 1
    print("✓ test_backpressure_callbacks")

def test_backpressure_chunks_batched():
    bp = BackpressureController()
    bp.record_chunks_batched(3, 300)
    assert bp.total_sent >= 300
    print("✓ test_backpressure_chunks_batched")

# ─────────────────────────────────────────────
# ChunkManager Tests
# ─────────────────────────────────────────────

def test_chunk_manager_get_chunk_size():
    cm = ChunkManager()
    assert 10 <= cm.get_chunk_size(Phase.SKELETON) <= 500
    print("✓ test_chunk_manager_get_chunk_size")

def test_chunk_manager_create_chunk():
    cm = ChunkManager()
    chunk = cm.create_chunk(content="Test", phase=Phase.SKELETON, chunk_index=0, total_in_phase=3)
    assert isinstance(chunk, Chunk) and chunk.content == "Test" and chunk.phase == Phase.SKELETON
    assert chunk.priority == 0
    print("✓ test_chunk_manager_create_chunk")

def test_chunk_manager_split_content():
    cm = ChunkManager(config=POSConfig(default_chunk_size=10, max_chunk_size=10))
    chunks = cm.split_content("This is a test of chunk splitting.", Phase.DETAILS)
    assert len(chunks) > 0
    print("✓ test_chunk_manager_split_content")

def test_chunk_manager_merge_chunks():
    cm = ChunkManager()
    chunks = [cm.create_chunk("Hello ", Phase.DETAILS, 0, 2), cm.create_chunk("world!", Phase.DETAILS, 1, 2)]
    merged = cm.merge_chunks(chunks)
    assert len(merged) <= 2
    print("✓ test_chunk_manager_merge_chunks")

def test_chunk_manager_phase_sizes():
    cm = ChunkManager()
    for phase in [Phase.SKELETON, Phase.DETAILS, Phase.CONCLUSION]:
        assert 10 <= cm.get_phase_chunks(phase) <= 500
    print("✓ test_chunk_manager_phase_sizes")

def test_chunk_manager_get_metrics():
    cm = ChunkManager()
    metrics = cm.get_metrics()
    assert "phase_chunk_sizes" in metrics and "history_size" in metrics and "backpressure_state" in metrics
    print("✓ test_chunk_manager_get_metrics")

# ─────────────────────────────────────────────
# ProgressiveRenderer Tests
# ─────────────────────────────────────────────

def test_renderer_render_yields_chunks():
    renderer = ProgressiveRenderer(config=ZERO_CONFIG)
    structure = PredictedStructure(skeleton="Skeleton", sections=[], conclusion="Conclusion", confidence=0.8)
    chunks = list(renderer.render(structure))
    assert len(chunks) > 0
    phases = set(c.phase for c in chunks)
    assert Phase.SKELETON in phases
    print("✓ test_renderer_render_yields_chunks")

def test_renderer_skeleton_before_conclusion():
    renderer = ProgressiveRenderer(config=ZERO_CONFIG)
    structure = PredictedStructure(skeleton="Skeleton", sections=[], conclusion="Conclusion", confidence=0.8)
    chunks = list(renderer.render(structure))
    skel_idx = [i for i, c in enumerate(chunks) if c.phase == Phase.SKELETON]
    concl_idx = [i for i, c in enumerate(chunks) if c.phase == Phase.CONCLUSION]
    if skel_idx and concl_idx:
        assert skel_idx[-1] < concl_idx[0]
    print("✓ test_renderer_skeleton_before_conclusion")

def test_renderer_callbacks():
    renderer = ProgressiveRenderer(config=ZERO_CONFIG)
    received = []
    renderer.register_callback("skeleton", lambda c: received.append(c))
    structure = PredictedStructure(skeleton="Test", sections=[], conclusion="End", confidence=0.8)
    list(renderer.render(structure))
    assert len(received) > 0
    print("✓ test_renderer_callbacks")

def test_renderer_render_async():
    renderer = ProgressiveRenderer(config=ZERO_CONFIG)
    received = []
    structure = PredictedStructure(skeleton="Test", sections=[], conclusion="End", confidence=0.8)
    renderer.render_async(structure, lambda c: received.append(c))
    assert len(received) > 0
    print("✓ test_renderer_render_async")

def test_renderer_get_render_stats():
    renderer = ProgressiveRenderer(config=ZERO_CONFIG)
    stats = renderer.get_render_stats()
    assert "total_chunks_rendered" in stats and "currently_rendering" in stats
    assert stats["currently_rendering"] is False
    print("✓ test_renderer_get_render_stats")

def test_renderer_reset():
    renderer = ProgressiveRenderer(config=ZERO_CONFIG)
    structure = PredictedStructure(skeleton="Test", sections=[], conclusion="End", confidence=0.8)
    list(renderer.render(structure))
    assert renderer._total_chunks_rendered > 0
    renderer.reset()
    assert renderer._total_chunks_rendered == 0 and renderer._rendering is False
    print("✓ test_renderer_reset")

def test_renderer_unknown_phase_callback():
    renderer = ProgressiveRenderer()
    try:
        renderer.register_callback("invalid_phase", lambda c: None)
        assert False
    except ValueError:
        pass
    print("✓ test_renderer_unknown_phase_callback")

def test_renderer_with_no_sections():
    renderer = ProgressiveRenderer(config=ZERO_CONFIG)
    structure = PredictedStructure(skeleton="Just skeleton", sections=[], conclusion="End", confidence=0.5)
    chunks = list(renderer.render(structure))
    assert len(chunks) > 0
    print("✓ test_renderer_with_no_sections")

# ─────────────────────────────────────────────
# Convenience Function Tests
# ─────────────────────────────────────────────

def test_create_pos_system():
    system = create_pos_system()
    assert "predictor" in system and "renderer" in system and "chunk_manager" in system
    assert "backpressure_controller" in system and "config" in system
    assert isinstance(system["predictor"], ResponsePredictor)
    assert isinstance(system["renderer"], ProgressiveRenderer)
    assert isinstance(system["chunk_manager"], ChunkManager)
    assert isinstance(system["backpressure_controller"], BackpressureController)
    print("✓ test_create_pos_system")

def test_stream_response():
    chunks = list(stream_response("How does photosynthesis work?"))
    assert len(chunks) > 0 and Phase.SKELETON in set(c.phase for c in chunks)
    print("✓ test_stream_response")

# ─────────────────────────────────────────────
# Integration Tests
# ─────────────────────────────────────────────

def test_full_pos_pipeline():
    system = create_pos_system(config=ZERO_CONFIG)
    predictor = system["predictor"]
    renderer = system["renderer"]
    structure = predictor.predict("Write a Python function for sorting")
    assert structure.skeleton != ""
    chunks = list(renderer.render(structure))
    assert len(chunks) > 0
    phases = set(c.phase for c in chunks)
    assert Phase.SKELETON in phases and Phase.CONCLUSION in phases
    print("✓ test_full_pos_pipeline")

def test_backpressure_affects_chunk_size():
    config = POSConfig(backpressure_window_size=5)
    bp = BackpressureController(config)
    cm = ChunkManager(config, bp)
    for _ in range(5):
        bp.record_chunk_sent(100)
    size_under = cm.get_chunk_size(Phase.DETAILS)
    bp_empty = BackpressureController(config)
    cm_empty = ChunkManager(config, bp_empty)
    size_normal = cm_empty.get_chunk_size(Phase.DETAILS)
    assert size_under <= size_normal
    print("✓ test_backpressure_affects_chunk_size")

def test_predictor_and_renderer_integration():
    predictor = ResponsePredictor()
    renderer = ProgressiveRenderer(config=ZERO_CONFIG)
    queries = ["How does photosynthesis work?", "Write a Python function", "Compare Python and Java"]
    for query in queries:
        structure = predictor.predict(query)
        chunks = list(renderer.render(structure))
        assert len(chunks) > 0 and structure.confidence > 0.0
    print("✓ test_predictor_and_renderer_integration")

def test_concurrent_access():
    predictor = ResponsePredictor()
    renderer = ProgressiveRenderer(config=ZERO_CONFIG)
    errors = []
    def worker():
        try:
            structure = predictor.predict("Test concurrent query")
            list(renderer.render(structure))
        except Exception as e:
            errors.append(e)
    threads = [threading.Thread(target=worker) for _ in range(5)]
    for t in threads: t.start()
    for t in threads: t.join()
    assert len(errors) == 0
    print("✓ test_concurrent_access")

def test_prediction_cache_thread_safety():
    cache = PredictionCache(max_size=100)
    errors = []
    def worker(idx):
        try:
            s = PredictedStructure(skeleton=f"T{idx}", sections=[], conclusion="c", confidence=0.5)
            cache.put(f"k{idx}", s)
            cache.get(f"k{idx}")
        except Exception as e:
            errors.append(e)
    threads = [threading.Thread(target=worker, args=(i,)) for i in range(20)]
    for t in threads: t.start()
    for t in threads: t.join()
    assert len(errors) == 0
    print("✓ test_prediction_cache_thread_safety")

# ─────────────────────────────────────────────
# Exception Tests
# ─────────────────────────────────────────────

def test_pos_prediction_error():
    try:
        raise POSPredictionError("Prediction failed")
    except POSPredictionError as e:
        assert str(e) == "Prediction failed"
    print("✓ test_pos_prediction_error")

def test_pos_rendering_error():
    try:
        raise POSRenderingError("Rendering failed")
    except POSRenderingError as e:
        assert str(e) == "Rendering failed"
    print("✓ test_pos_rendering_error")

def test_pos_backpressure_error():
    try:
        raise POSBackpressureError("Backpressure exceeded")
    except POSBackpressureError as e:
        assert str(e) == "Backpressure exceeded"
    print("✓ test_pos_backpressure_error")

# ─────────────────────────────────────────────
# Edge Case Tests
# ─────────────────────────────────────────────

def test_chunk_manager_empty_content():
    cm = ChunkManager()
    assert cm.split_content("", Phase.DETAILS) == []
    print("✓ test_chunk_manager_empty_content")

def test_chunk_manager_merge_empty():
    cm = ChunkManager()
    assert cm.merge_chunks([]) == []
    print("✓ test_chunk_manager_merge_empty")

def test_backpressure_zero_window():
    config = POSConfig(backpressure_window_size=1)
    bp = BackpressureController(config)
    bp.record_chunk_sent(10)
    assert bp.get_utilization() > 0
    print("✓ test_backpressure_zero_window")

def test_predictor_long_query():
    predictor = ResponsePredictor()
    long_query = "how " * 50 + "does this work exactly"
    structure = predictor.predict(long_query)
    assert structure.skeleton != "" and structure.confidence > 0.0
    print("✓ test_predictor_long_query")

def test_chunk_priority_ordering():
    cm = ChunkManager()
    skel = cm.create_chunk("skel", Phase.SKELETON, 0, 1)
    detail = cm.create_chunk("detail", Phase.DETAILS, 0, 1)
    conc = cm.create_chunk("conc", Phase.CONCLUSION, 0, 1)
    assert skel.priority < detail.priority < conc.priority
    print("✓ test_chunk_priority_ordering")

# ─────────────────────────────────────────────
# Run all tests
# ─────────────────────────────────────────────

if __name__ == "__main__":
    tests = [
        # Config
        test_pos_config_defaults, test_pos_config_custom, test_pos_config_as_dict,
        # Enums
        test_phase_enum_values, test_prediction_confidence_enum, test_chunk_priority_enum, test_predictor_mode_enum,
        # Data classes
        test_section_prediction, test_predicted_structure, test_predicted_structure_to_dict, test_chunk_dataclass, test_chunk_metrics,
        # PredictionCache
        test_prediction_cache_put_get, test_prediction_cache_miss, test_prediction_cache_lru_eviction, test_prediction_cache_invalidate, test_prediction_cache_clear,
        # ResponsePredictor
        test_predictor_predict_scientific, test_predictor_predict_coding, test_predictor_predict_comparison, test_predictor_predict_definition, test_predictor_predict_list, test_predictor_predict_debug, test_predictor_predict_empty_raises, test_predictor_predict_non_string_raises, test_predictor_caching, test_predictor_cache_miss_increments, test_predictor_with_confidence, test_predictor_get_stats, test_predictor_reset_stats, test_predictor_mode_setting, test_predictor_unknown_query,
        # BackpressureController
        test_backpressure_initial_state, test_backpressure_record_chunk, test_backpressure_record_drop, test_backpressure_utilization, test_backpressure_rate_adjustment, test_backpressure_adjust_chunk_size, test_backpressure_get_current_rate, test_backpressure_callbacks, test_backpressure_chunks_batched,
        # ChunkManager
        test_chunk_manager_get_chunk_size, test_chunk_manager_create_chunk, test_chunk_manager_split_content, test_chunk_manager_merge_chunks, test_chunk_manager_phase_sizes, test_chunk_manager_get_metrics,
        # ProgressiveRenderer
        test_renderer_render_yields_chunks, test_renderer_skeleton_before_conclusion, test_renderer_callbacks, test_renderer_render_async, test_renderer_get_render_stats, test_renderer_reset, test_renderer_unknown_phase_callback, test_renderer_with_no_sections,
        # Convenience
        test_create_pos_system, test_stream_response,
        # Integration
        test_full_pos_pipeline, test_backpressure_affects_chunk_size, test_predictor_and_renderer_integration, test_concurrent_access, test_prediction_cache_thread_safety,
        # Exceptions
        test_pos_prediction_error, test_pos_rendering_error, test_pos_backpressure_error,
        # Edge cases
        test_chunk_manager_empty_content, test_chunk_manager_merge_empty, test_backpressure_zero_window, test_predictor_long_query, test_chunk_priority_ordering,
    ]

    passed = failed = 0
    errors = []
    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            failed += 1
            errors.append(f"{test.__name__}: {e}")

    print(f"\n{'='*60}")
    print(f"Results: {passed} passed, {failed} failed out of {len(tests)} tests")
    print(f"{'='*60}")
    if errors:
        for err in errors:
            print(f"  FAIL: {err}")
    else:
        print("All tests passed!")
