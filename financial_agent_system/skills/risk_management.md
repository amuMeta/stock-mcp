# 风险管理技能

## 描述
该技能用于评估投资组合的风险水平，包括波动性、最大回撤、夏普比率等指标。

## 输入参数
- portfolio: 投资组合配置（包含资产类型和权重）
- risk_free_rate: 无风险利率
- time_horizon: 投资时间 horizon

## 执行步骤
1. 分析投资组合的资产配置
2. 计算风险指标
3. 评估风险水平
4. 生成风险报告和建议

## 输出格式
```json
{
  "portfolio": 投资组合配置,
  "risk_metrics": {
    "volatility": 波动性,
    "max_drawdown": 最大回撤,
    "sharpe_ratio": 夏普比率
  },
  "risk_level": "风险等级",
  "recommendations": [
    "建议1",
    "建议2",
    "建议3"
  ],
  "analysis": "详细风险分析报告"
}
```
