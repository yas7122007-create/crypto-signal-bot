"""V2 analysis pipeline: bars from the Rust market-data engine, PatchTST forecasts, Toto
validation, Nemotron review and deterministic ranking. Analysis and paper trading only.

Standard-library modules (contracts, bridge, workers, ranking, journal, evaluate,
pipeline) are safe to import from the bot. dataset/baselines need numpy; patchtst and the
model workers need torch (requirements-ml.txt).
"""
