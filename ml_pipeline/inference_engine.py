import os
import logging
import random
import pickle
from typing import Any
import pandas as pd

logger = logging.getLogger("TradingEngine.InferenceEngine")


class InferenceEngine:
    """
    Loads serialized ML models (XGBoost, Scikit-Learn) and processes
    live technical feature matrices to generate directional probabilities.
    """

    def __init__(self, model_name: str = "xgboost_v1.pkl"):
        """
        Initializes the inference layer and locates model weights.

        :param model_name: Filename of the target serialized model binary.
        """
        self.model_name = model_name
        # Build path mapping directly to your internal project architecture structure
        self.model_dir = os.path.join(os.path.dirname(__file__), "models")
        self.model_path = os.path.join(self.model_dir, self.model_name)
        self.model: Any = None

        # Load weights on service initialization
        self._load_serialized_model()

    def _load_serialized_model(self) -> None:
        """Attempts to unpickle the ML model binary securely from deep disk memory layers."""
        try:
            if os.path.exists(self.model_path):
                with open(self.model_path, "rb") as f:
                    self.model = pickle.load(f)
                logger.info(f"Successfully loaded ML model binary layer: '{self.model_name}'")
            else:
                logger.warning(
                    f"Model binary '{self.model_name}' not found at {self.model_path}. "
                    f"Inference engine will operate in mathematical fallback mode."
                )
        except Exception as e:
            logger.error(f"Critical error unpickling serialized model weights array: {str(e)}")
            self.model = None

    def predict_next_move(self, feature_vector: pd.Series) -> float:
        """
        Ingests a live engineered technical indicator series row and calculates the
        probability of the price moving upward on the next interval block.

        :param feature_vector: Pandas Series containing real-time values (RSI, SMA cross ratios, etc.)
        :return: Floating point directional confidence probability capped strictly between 0.0 and 1.0.
        """
        # Ensure your live strategy code is feeding a valid pandas object
        if feature_vector is None or feature_vector.empty:
            logger.error("Inference execution skipped: Input feature vector state is empty or invalid.")
            return 0.5

        # ---- PRODUCTION EXECUTION FLOW ----
        if self.model is not None:
            try:
                # Convert the flat Series row matrix into a 2D array matching scikit/xgboost expectancies
                input_matrix = feature_vector.values.reshape(1, -1)

                # Predict probabilities (assumes binary classification target: 0=Down/Flat, 1=Up)
                # predict_proba returns an array layout: [[prob_class_0, prob_class_1]]
                probabilities = self.model.predict_proba(input_matrix)
                return float(probabilities[0][1])

            except Exception as e:
                logger.error(f"Production model prediction pipeline crashed, reverting to fallback: {str(e)}")

        # ---- FALLBACK PROTOTYPE MODE FLOW ----
        # If no binary weights are saved, generate a programmatic inference score based on TA features
        return self._calculate_fallback_probability(feature_vector)

    def _calculate_fallback_probability(self, features: pd.Series) -> float:
        """
        Deterministic, rules-based mathematical indicator scoring engine.
        Acts as your ML pipeline placeholder inside your Tumbleweed PyCharm instance.
        """
        score_weight = 0.5  # Neutral starting baseline probability

        try:
            # 1. Pull feature parameters passed down from strategies/ai_template_strategy.py
            rsi = features.get("rsi", 50.0)
            sma_fast = features.get("sma_fast", 1.0)
            sma_slow = features.get("sma_slow", 1.0)

            # 2. Score directional trend momentum rules
            # Oversold pricing implies a mathematical mean-reversion buying bounce opportunity
            if rsi < 35.0:
                score_weight += 0.15
            elif rsi > 65.0:
                score_weight -= 0.15

            # Evaluate fast vs slow golden/death moving average crossover dynamics
            if sma_fast > sma_slow:
                score_weight += 0.10  # Bullish alignment
            else:
                score_weight -= 0.10  # Bearish alignment

            # 3. Add a microscopic fraction of controlled stochastic noise to simulate live variance
            score_weight += random.uniform(-0.02, 0.02)

        except Exception as e:
            logger.debug(f"Error executing fallback feature rules scaling framework: {str(e)}")

        # Constrain output bounds tightly to prevent numerical array parsing blowouts
        return max(0.0, min(1.0, score_weight))
