#!/bin/bash

# 部署脚本

echo "=== 金融智能体系统部署脚本 ==="
echo ""

echo "选择部署模式:"
echo "1. 本地开发环境"
echo "2. Docker容器化部署"
echo "3. Kubernetes集群部署"
echo ""

read -p "请输入选项 (1-3): " choice

echo ""

case $choice in
    1)
        echo "=== 本地开发环境部署 ==="
        echo "安装依赖..."
        pip install -r requirements.txt
        echo "启动服务..."
        python main.py
        ;;
    2)
        echo "=== Docker容器化部署 ==="
        echo "构建Docker镜像..."
        docker build -t financial-agent-system .
        echo "运行Docker容器..."
        docker run -d -p 8000:8000 --name financial-agent financial-agent-system
        echo "服务已启动在 http://localhost:8000"
        ;;
    3)
        echo "=== Kubernetes集群部署 ==="
        echo "创建Kubernetes部署..."
        kubectl apply -f k8s/deployment.yaml
        echo "创建Kubernetes服务..."
        kubectl apply -f k8s/service.yaml
        echo "服务已部署到Kubernetes集群"
        ;;
    *)
        echo "无效的选项"
        exit 1
        ;;
esac
