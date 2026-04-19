from __future__ import annotations

from fastapi import APIRouter, HTTPException, status

from src.server.capabilities.technical.schemas import (
    CalculateSupportResistanceRequest,
    CalculateTechnicalIndicatorsRequest,
    GenerateTradingSignalRequest,
)
from src.server.capabilities.technical.service import get_technical_capability_service
from src.server.runtime.models import RuntimeContext
from src.server.utils.logger import logger


def build_router(runtime: RuntimeContext) -> APIRouter:
    router = APIRouter(prefix="/api/v1/technical", tags=["Technical"])
    service = get_technical_capability_service(runtime)

    @router.post("/indicators/calculate", summary="计算技术指标")
    async def calculate_technical_indicators(
        request: CalculateTechnicalIndicatorsRequest,
    ):
        try:
            logger.info(
                "Capability API: calculate_technical_indicators",
                symbol=request.symbol,
                period=request.period,
                interval=request.interval,
            )
            result = await service.calculate_technical_indicators(
                request.symbol, request.period, request.interval
            )

            if isinstance(result, dict) and result.get("error"):
                error_msg = result.get("error", "Unknown error")
                source = result.get("source", "unknown")
                if "No result" in error_msg or "does not support" in error_msg:
                    return {
                        "error": f"技术指标功能当前不可用",
                        "message": f"当前使用的数据源（{source}）不支持技术指标计算。Tushare数据源暂时不可用或未配置。",
                        "solution": "请配置有效的Tushare API Token以使用技术指标功能。",
                        "symbol": request.symbol,
                        "source": source,
                    }
                return result
            return result
        except Exception as exc:
            logger.error(
                f"Capability API error in calculate_technical_indicators: {exc}",
                exc_info=True,
            )
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to calculate indicators: {exc}",
            ) from exc

    @router.post("/signals/trading", summary="生成交易信号")
    async def generate_trading_signal(request: GenerateTradingSignalRequest):
        try:
            logger.info(
                "Capability API: generate_trading_signal",
                symbol=request.symbol,
                period=request.period,
                interval=request.interval,
            )
            result = await service.generate_trading_signal(
                request.symbol, request.period, request.interval
            )

            if isinstance(result, dict) and result.get("error"):
                error_msg = result.get("error", "Unknown error")
                source = result.get("source", "unknown")
                if "No result" in error_msg or "does not support" in error_msg:
                    return {
                        "error": f"交易信号功能当前不可用",
                        "message": f"当前使用的数据源（{source}）不支持交易信号生成。Tushare数据源暂时不可用或未配置。",
                        "solution": "请配置有效的Tushare API Token以使用交易信号功能。",
                        "symbol": request.symbol,
                        "source": source,
                    }
                return result
            return result
        except Exception as exc:
            logger.error(
                f"Capability API error in generate_trading_signal: {exc}", exc_info=True
            )
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to generate trading signal: {exc}",
            ) from exc

    @router.post("/analysis/support-resistance", summary="计算支撑阻力位")
    async def calculate_support_resistance(request: CalculateSupportResistanceRequest):
        try:
            logger.info(
                "Capability API: calculate_support_resistance",
                symbol=request.symbol,
                period=request.period,
            )
            result = await service.calculate_support_resistance(
                request.symbol, request.period
            )

            if isinstance(result, dict) and result.get("error"):
                error_msg = result.get("error", "Unknown error")
                source = result.get("source", "unknown")
                if "No result" in error_msg or "does not support" in error_msg:
                    return {
                        "error": f"支撑阻力位功能当前不可用",
                        "message": f"当前使用的数据源（{source}）不支持支撑阻力位计算。Tushare数据源暂时不可用或未配置。",
                        "solution": "请配置有效的Tushare API Token以使用支撑阻力位功能。",
                        "symbol": request.symbol,
                        "source": source,
                    }
                return result
            return result
        except Exception as exc:
            logger.error(
                f"Capability API error in calculate_support_resistance: {exc}",
                exc_info=True,
            )
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to calculate support/resistance: {exc}",
            ) from exc

    return router
