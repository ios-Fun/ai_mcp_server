# server.py
from fastapi import FastAPI, Request
from sse_starlette.sse import EventSourceResponse
from mcp.server.fastmcp import FastMCP
from mcp.server.sse import SseServerTransport
from starlette.routing import Mount, Route
from starlette.applications import Starlette
from starlette.responses import Response
from typing import Optional, List, Dict, Any
from src_deerflow.redis_client import memoryRedis
from concurrent.futures import ThreadPoolExecutor, as_completed
# import clickhouse_connect
import asyncio
import json
import os
from dotenv import load_dotenv
import requests
import logging
import re

load_dotenv()

# Initialize FastAPI app
app = FastAPI()

# Initialize MCP server
mcp = FastMCP("ClickHouse MCP Server")
server_url = os.getenv("SERVER_URL")

# 配置日志
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Define an MCP tool to execute ClickHouse queries


# 获取指定节点下相关的标杆信息可能有多个 -- benchmarkTarget
@mcp.tool()
def benchmarkTarget(nodeId: Optional[int] = None) -> str:
    """
    获取指定节点下相关的标杆信息
    根据传入的目标，查询对应的标杆

    Args:
        nodeId: 实体节点id
    Returns:
        str: 返回指定实体节点对应的目标信息json对象
            targetId: 标杆目标ID
            name: 目标业务名称
            direction: 指标优化方向：越大越好/越小越好
            unit: 指标计量单位，如%、t/h
            isVariance: 是否开启偏差校验，是/否
            objectId: 标杆id
            varianceValue: 允许偏差阈值
            tagCode: 测点标签编码
            tagName: 测点中文名称，业务指标
    """
    param = {}
    param["nodeId"] = nodeId
    logging.info(f"param: {nodeId}")

    java_url = os.getenv("B_SERVER_URL") + "/benchmark/target"
    logging.info(f"java_url: {java_url}")
    response = requests.post(url=java_url, json=param, headers={"Content-Type": "application/json"})
    logging.info(f"response.status_code: {response.text}")
    if response.status_code == 200:
        return response.text
    else:
        return "发送失败"

# 找到同一目标同工况下评估单因素对比信息 -- benchmarkEvaluation
@mcp.tool()
def benchmarkEvaluation(nodeId: Optional[int] = None) -> str:
    """
    获取指定节点下相关的因素信息
    根据传入的标杆，查询对应的因素

    Args:
        nodeId: 实体节点id
    Returns:
        str: 返找到同一目标同工况下评估单因素对比信息
    """
    param = {}
    param["nodeId"] = nodeId
    logging.info(f"param: {nodeId}")

    java_url = os.getenv("B_SERVER_URL") + "/benchmark/evaluation"
    logging.info(f"java_url: {java_url}")
    response = requests.post(url=java_url, json=param, headers={"Content-Type": "application/json"})
    logging.info(f"response.status_code: {response.text}")
    if response.status_code == 200:
        return response.text
    else:
        return "发送失败"

# Create SSE transport
transport = SseServerTransport("/messages/")

# Define SSE handler
async def handle_sse(request):
    async with transport.connect_sse(
            request.scope, request.receive, request._send
    ) as streams:
        await mcp._mcp_server.run(
            streams[0], streams[1], mcp._mcp_server.create_initialization_options()
        )
        return Response()

# Create Starlette routes
routes = [
    Route("/sse", endpoint=handle_sse),
    Mount("/messages", app=transport.handle_post_message),
]

# Create Starlette app
sse_app = Starlette(routes=routes)

# Mount the SSE app to the main FastAPI app
app.mount("/", sse_app)

# Run the application
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("benchmerk_mcp_server:app", host="0.0.0.0", port=int(os.getenv("B_PORT")), reload=True)
