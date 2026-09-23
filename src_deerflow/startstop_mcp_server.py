# server.py
from fastapi import FastAPI, Request
from sse_starlette.sse import EventSourceResponse
from mcp.server.fastmcp import FastMCP
from mcp.server.sse import SseServerTransport
from starlette.routing import Mount, Route
from starlette.applications import Starlette
from starlette.responses import Response
from typing import Optional, List, Dict, Any
# from src_deerflow.redis_client import memoryRedis
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


# 统计指定时间范围内启动次数、停机次数，并补充累计运行时长、停机时长
import os
import logging
import requests
from typing import Optional

@mcp.tool()
def startStopStatic(
        startTime: str,
        endTime: str,
        nodeId: Optional[int] = None
) -> str:
    """
    统计指定时间范围内启动次数、停机次数，并补充累计运行时长、停机时长

    Args:
        startTime: 开始时间字符串
        endTime: 结束时间字符串
        nodeId: 实体节点id
    Returns:
        str: 指定时间范围内启动次数、停机次数
    """
    param = {
        "startTime": startTime,
        "endTime": endTime,
        "nodeId": nodeId
    }
    logging.info(f"param: {param}")

    java_url = os.getenv("S_SERVER_URL") + "/startStop/statistics"
    logging.info(f"java_url: {java_url}")
    try:
        response = requests.post(
            url=java_url,
            json=param,
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        logging.info(f"response.status_code: {response.status_code}, response.text: {response.text}")
        if response.status_code == 200:
            return response.text
        else:
            return f"发送失败，状态码：{response.status_code}"
    except Exception as e:
        logging.error(f"请求异常: {str(e)}")
        return f"请求异常：{str(e)}"



@mcp.tool()
def startStopDetails(
        resultId: str
) -> str:
    """
   查询某一次启停记录详情

    Args:
        resultId: 记录id
    Returns:
        str: 返回启停记录详情
    """
    param = {
        "resultId": resultId
    }
    logging.info(f"param: {param}")

    java_url = os.getenv("S_SERVER_URL") + "/startStop/details"
    logging.info(f"java_url: {java_url}")
    try:
        response = requests.post(
            url=java_url,
            json=param,
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        logging.info(f"response.status_code: {response.status_code}, response.text: {response.text}")
        if response.status_code == 200:
            return response.text
        else:
            return f"发送失败，状态码：{response.status_code}"
    except Exception as e:
        logging.error(f"请求异常: {str(e)}")
        return f"请求异常：{str(e)}"


@mcp.tool()
def getStartStopTotalMaterial(
        resultId: str
) -> str:
    """
   查询某一次启停物料

    Args:
        resultId: 记录id
    Returns:
        str: 返回启停物料信息
    """
    param = {
        "resultId": resultId
    }
    logging.info(f"param: {param}")

    java_url = os.getenv("S_SERVER_URL") + "/startStop/material/details"
    logging.info(f"java_url: {java_url}")
    try:
        response = requests.post(
            url=java_url,
            json=param,
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        logging.info(f"response.status_code: {response.status_code}, response.text: {response.text}")
        if response.status_code == 200:
            return response.text
        else:
            return f"发送失败，状态码：{response.status_code}"
    except Exception as e:
        logging.error(f"请求异常: {str(e)}")
        return f"请求异常：{str(e)}"

@mcp.tool()
def startStopRecord(
        nodeId: Optional[int] = None
) -> str:
    """
   查询最近10条启停记录

    Args:
        resultId: 记录id
    Returns:
        str: 返回最近10条启停记录
    """
    param = {
        "nodeId": nodeId
    }
    logging.info(f"param: {param}")

    java_url = os.getenv("S_SERVER_URL") + "/startStop/record"
    logging.info(f"java_url: {java_url}")
    try:
        response = requests.post(
            url=java_url,
            json=param,
            timeout=10
        )
        logging.info(f"response.status_code: {response.status_code}, response.text: {response.text}")
        if response.status_code == 200:
            return response.text
        else:
            return f"发送失败，状态码：{response.status_code}"
    except Exception as e:
        logging.error(f"请求异常: {str(e)}")
        return f"请求异常：{str(e)}"
@mcp.tool()
def stageRecord(
        eventName: str,
        nodeId: Optional[int] = None
) -> str:
    """
   启停阶段识别

    Args:
        nodeId: 节点id
        eventName：事件类型启动/停止
    Returns:
        str: 返回启动各阶段的完成情况和总耗时
    """
    param = {
        "nodeId": nodeId,
        "eventName": eventName
    }
    logging.info(f"param: {param}")

    java_url = os.getenv("S_SERVER_URL") + "/startStop/stage/record"
    logging.info(f"java_url: {java_url}")
    try:
        response = requests.post(
            url=java_url,
            json=param,
            timeout=10
        )
        logging.info(f"response.status_code: {response.status_code}, response.text: {response.text}")
        if response.status_code == 200:
            return response.text
        else:
            return f"发送失败，状态码：{response.status_code}"
    except Exception as e:
        logging.error(f"请求异常: {str(e)}")
        return f"请求异常：{str(e)}"

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
    uvicorn.run("startstop_mcp_server:app", host="0.0.0.0", port=int(os.getenv("S_PORT")), reload=True)
