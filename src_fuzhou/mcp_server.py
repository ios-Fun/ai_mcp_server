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
def verifyCleaningEffect() -> str:
    """根据即时数据与目前的灰尘覆盖率，您的光伏场站即将达到最佳清洗时机。系统建议七天后进行清洗，请提早规划。"""
    logging.info("1")
    return json.dumps({"text":"1"})

@mcp.tool()
def diagnoseAndRepairJunctionBoxIssues(entity: str) -> str:
    """在清洗过程中发现十号方阵七号逆变器PV4接口组件清洁后仍有明显印记和条纹，请调出对应趋势图并分析。
    Args: 
        entity: 部件名称（如“十号方阵七号逆变器PV4接口组件”）
    Returns:
        类型和参数的字典
    """
    logging.info("2")
    params = {"entity": entity}
    return getHttpResult("question4")

@mcp.tool()
def generateCleaningEffectReport(entity: str) -> str:
    """清洗过程中发现，七号方阵二号逆变器的外壳通风口灰尘覆盖，帮我看一下该逆变器温度是否异常及相关处理方式？
    Args: 
        entity: 部件名称（如“七号方阵二号逆变器的外壳”）
    Returns:
        类型和参数的字典
    """
    logging.info("3")
    params = {"entity": entity}
    return getHttpResult("question2")

@mcp.tool()
def suggestSaltSprayResistanceUpgrades() -> str:
    """帮我生成一份本次清洗效果报告"""
    logging.info("4")
    return json.dumps({"text":"4"})

@mcp.tool()
def suggestSaltSprayResistanceUpgrades() -> str:
    """帮我生成一份本次清洗效果报告"""
    logging.info("4")
    return json.dumps({"text":"4"})

@mcp.tool()
def checkAshLossAndCleaningNeed(entity: str) -> str:
    """帮我查看五号方阵的灰损情况，是否安排需要清洗？
    Args: 
        entity: 部件名称（如“五号方阵”）
    Returns:
        类型和参数的字典
    """
    logging.info("5")
    params = {"entity": entity}
    return getHttpResult("question5")

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
