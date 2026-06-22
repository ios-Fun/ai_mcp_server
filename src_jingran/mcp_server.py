# server.py
from fastapi import FastAPI, Request
from sse_starlette.sse import EventSourceResponse
from mcp.server.fastmcp import FastMCP
from mcp.server.sse import SseServerTransport
from starlette.routing import Mount, Route
from starlette.applications import Starlette
import clickhouse_connect
import asyncio
import json
import os
from dotenv import load_dotenv
import requests
import logging
from requests import ConnectTimeout

load_dotenv()

# Initialize FastAPI app
app = FastAPI()

# Initialize MCP server
mcp = FastMCP("ClickHouse MCP Server")

# Define an MCP tool to execute ClickHouse queries
@mcp.tool()
def actionShowTrend(time: str) -> str:
    """看一下趋势
    Args: 
        entity: 时间
    Returns:
        类型和参数的字典
    """
    logging.info("1")
    return json.dumps({"text":"1"})

@mcp.tool()
def actionQueryDetail(entity: str) -> str:
    """看一下详情
    Args: 
        entity: 部件名称
    Returns:
        类型和参数的字典
    """
    logging.info("22")
    return json.dumps({"text":"22"})

@mcp.tool()
def actionDeviceFault(entity: str) -> str:
    """京燃机组发生了某某故障
    Args: 
        entity: 部件名称
    Returns:
        类型和参数的字典
    """
    logging.info("21")
    return json.dumps({"text":"21"})
    # params = {"entity": entity}
    # return getHttpResult("question2")

@mcp.tool()
def actionDeviceFaultCount(entity: str) -> str:
    """统计设备频发故障
    Args: 
        entity: 部件名称
    Returns:
        类型和参数的字典
    """
    logging.info("6")
    return json.dumps({"text":"6"})

@mcp.tool()
def actionPlatformDeviceCount(entity: str) -> str:
    """系统接入了哪些设备
    Args: 
        entity: 部件名称
    Returns:
        类型和参数的字典
    """
    logging.info("8")
    return json.dumps({"text":"8"})

@mcp.tool()
def actionPlatformStatus(entity: str) -> str:
    """目前机组运行如何
    Args: 
        entity: 部件名称
    Returns:
        类型和参数的字典
    """
    logging.info("17")
    return json.dumps({"text":"17"})

@mcp.tool()
def actionPowerplantDiagnosis(entity: str) -> str:
    """我想看一下当前有哪些预警信息，诊断信息
    Args: 
        entity: 部件名称
    Returns:
        类型和参数的字典
    """
    logging.info("18")
    return json.dumps({"text":"18"})

@mcp.tool()
def actionDeviceFaultmode(entity: str) -> str:
    """设备的故障模式
    Args: 
        entity: 部件名称
    Returns:
        类型和参数的字典
    """
    logging.info("11")
    return json.dumps({"text":"11"})

@mcp.tool()
def actionSameFault(entity: str) -> str:
    """同类型故障
    Args: 
        entity: 部件名称
    Returns:
        类型和参数的字典
    """
    logging.info("12")
    return json.dumps({"text":"12"})

@mcp.tool()
def actionDeviceCommonFault(entity: str) -> str:
    """设备常见的故障有哪些
    Args: 
        entity: 部件名称
    Returns:
        类型和参数的字典
    """
    logging.info("13")
    return json.dumps({"text":"13"})

@mcp.tool()
def actionQueryDiagnosisDetail(entity: str) -> str:
    """查询某一条诊断单详情 
    Args: 
        entity: 诊断单号
    Returns:
        类型和参数的字典
    """
    logging.info("19")
    return json.dumps({"text":"19"})


def getHttpResult(funStr: str) -> str:
    url = "http://10.237.201.18:8083/api-AIAssistant/runnerhelper/"+funStr
    logging.info(url)
    try:
        response = requests.post(url=url, params=None, headers=None)
        return response.text
    except Exception as e:
        return "访问异常"


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
    uvicorn.run("mcp_server:app", host="0.0.0.0", port=int(os.getenv("PORT")), reload=True)
