# ==============================================================================
# 🔄 LAYER 2: GLOBAL STRATEGY STATE & MUTABILITY RUNTIME MANAGER (CLEANED)
# ==============================================================================
import logging
from typing import Dict, Any

# Clear imports pulling each concrete class from its individual standalone file asset [INDEX]
from strategies.base_strategy import BaseStrategy
from strategies.ai_template_strategy import AITemplateStrategy
from strategies.mean_rev_strategy import MeanRevStrategy

logger = logging.getLogger("TradingEngine.StrategyManager")


class StrategyRuntimeManager:
    """
    Tracks and hot-swaps active execution algorithm instances mid-flight.
    Supports clean multi-file dynamic compilation via a factory registry [INDEX].
    """

    def __init__(self) -> None:
        # Map class templates directly to clear, decoupled factory string keys [INDEX]
        self._strategy_registry = {
            "AI_ALPHA_V1": AITemplateStrategy,
            "MEAN_REV_V2": MeanRevStrategy
        }

        initial_params = {"rsi_period": 14, "rsi_oversold": 30.0, "rsi_overbought": 70.0}
        # Cold boot baseline defaults to your core machine learning AI strategy layer
        self.active_strategy: BaseStrategy = AITemplateStrategy(strategy_id="AI_ALPHA_V1", parameters=initial_params)

    def switch_strategy(self, strategy_id: str, parameters: Dict[str, Any]) -> None:
        """Hot-swaps the underlying strategy logic instance safely inside memory loops [INDEX]."""
        logger.info(f"🔄 Hot-swapping background strategy context: {self.active_strategy.strategy_id} ──► {strategy_id}")

        # Dynamically look up the target class using the factory registry map contract [INDEX]
        strategy_class = self._strategy_registry.get(strategy_id)

        if strategy_class is not None:
            # Instantiate the specific isolated file component class dynamically [INDEX]
            self.active_strategy = strategy_class(strategy_id=strategy_id, parameters=parameters)
            # Hard-enforce activation flags so the loop passes initial check blocks immediately
            self.active_strategy.is_enabled = True
        else:
            logger.error(
                f"❌ [FACTORY FAULT] Requested strategy token '{strategy_id}' is unknown in registry. Fallback ignored.")

    def hot_swap_strategy(self, strategy_id: str, new_parameters: Dict[str, Any]) -> None:
        """Alias wrapper mapping support to protect cross-script configuration routes [INDEX]."""
        self.switch_strategy(strategy_id=strategy_id, parameters=new_parameters)


# ✅ THE ONLY SINGLETON WE NEED: Exposed globally to the lifecycle context loops [INDEX]
strategy_manager = StrategyRuntimeManager()
