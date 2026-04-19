# 投资建议技能

## 描述
该技能用于根据用户的风险偏好和投资目标，生成个性化的投资建议。

## 输入参数
- risk_profile: 风险偏好（保守、中等、激进）
- investment_goals: 投资目标列表
- time_horizon: 投资时间 horizon
- budget: 投资预算

## 执行步骤
1. 分析用户的风险偏好和投资目标
2. 制定投资策略
3. 推荐资产配置
4. 生成投资建议报告

## 输出格式
```json
{
  "risk_profile": "风险偏好",
  "investment_goals": 投资目标列表,
  "time_horizon": "投资时间 horizon",
  "asset_allocation": {
    "stocks": 股票配置比例,
    "bonds": 债券配置比例,
    "cash": 现金配置比例,
    "alternative": 另类资产配置比例
  },
  "recommendations": [
    "建议1",
    "建议2",
    "建议3"
  ],
  "analysis": "详细投资建议报告"
}
```
