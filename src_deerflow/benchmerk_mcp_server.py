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
# 获取指定时间点的所有评估单信息 -- getEvaluationList
@mcp.tool()
def getEvaluationList(unitId,startDate: Optional[str] = None,endDate: Optional[str] = None) -> str:
    """
    获取指定时间点的所有评估单信息

    Args:
        unitId（非必填）机组ID
        startDate: 开始时间  格式：2026-09-01 01:00:00
        endDate: 结束时间  格式：2026-09-01 01:00:00

    Returns:
        str: 获取指定时间点的所有评估单信息
    """
    param = {}
    param["unitId"] = unitId
    param["startDate"] = startDate
    param["endDate"] = endDate

    logging.info(f"param: {startDate},{endDate}")

    java_url = os.getenv("B_SERVER_URL") + "/benchmark/getEvaluationList"
    logging.info(f"java_url: {java_url}")
    response = requests.post(url=java_url, json=param, headers={"Content-Type": "application/json"})
    logging.info(f"response.status_code: {response.text}")
    if response.status_code == 200:
        return response.text
    else:
        return "发送失败"
@mcp.tool()
def getBenchmarkByTagCode(tagCode) -> str:
    """
    获取指标的标杆值和实际值

    Args:
        tagCode（非必填）指标编码

    Returns:
        str: 获取指标的标杆值和实际值
    """
    param = {}
    param["tagCode"] = tagCode

    logging.info(f"param: {tagCode}")

    java_url = os.getenv("B_SERVER_URL") + "/benchmark/getBenchmarkByTagCode"
    logging.info(f"java_url: {java_url}")
    response = requests.post(url=java_url, json=param, headers={"Content-Type": "application/json"})
    logging.info(f"response.status_code: {response.text}")
    if response.status_code == 200:
        return response.text
    else:
        return "发送失败"
@mcp.tool()
def getRangeValue(evaluationId) -> str:
    """
    获取单子的工况测点信息

    Args:
        evaluationId 寻优单Id

    Returns:
        str: 获取单子的工况测点信息
    """
    param = {}
    param["evaluationId"] = evaluationId

    logging.info(f"param: {evaluationId}")

    java_url = os.getenv("B_SERVER_URL") + "/benchmark/getRangeValue"
    logging.info(f"java_url: {java_url}")
    response = requests.post(url=java_url, json=param, headers={"Content-Type": "application/json"})
    logging.info(f"response.status_code: {response.text}")
    if response.status_code == 200:
        return response.text
    else:
        return "发送失败"
@mcp.tool()
def getLastEvaluation(evaluationId) -> str:
    """
    获取历史和最新的寻优单记录

    Args:
        evaluationId 寻优单Id

    Returns:
        str: 获取历史和最新的寻优单记录
    """
    param = {}
    param["evaluationId"] = evaluationId

    logging.info(f"param: {evaluationId}")

    java_url = os.getenv("B_SERVER_URL") + "/benchmark/getLastEvaluation"
    logging.info(f"java_url: {java_url}")
    response = requests.post(url=java_url, json=param, headers={"Content-Type": "application/json"})
    logging.info(f"response.status_code: {response.text}")
    if response.status_code == 200:
        return response.text
    else:
        return "发送失败"
@mcp.tool()
def getEvaluationByTagCode(tagCode,startDate: Optional[str] = None,endDate: Optional[str] = None) -> str:
    """
    获取指标的目标范围因素信息

    Args:
        tagCode 测点/指标编码
        startDate: 开始时间 （非必填） 格式：2026-09-01 01:00:00
        endDate: 结束时间 （非必填） 格式：2026-09-01 01:00:00

    Returns:
        str: 获取指标的目标范围因素信息
    """
    param = {}
    param["tagCode"] = tagCode
    param["startDate"] = startDate
    param["endDate"] = endDate

    logging.info(f"param: {startDate},{endDate}")

    java_url = os.getenv("B_SERVER_URL") + "/benchmark/getEvaluationByTagCode"
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
