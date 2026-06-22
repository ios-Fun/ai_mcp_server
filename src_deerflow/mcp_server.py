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

load_dotenv()

# Initialize FastAPI app
app = FastAPI()

# Initialize MCP server
mcp = FastMCP("ClickHouse MCP Server")

# Define an MCP tool to execute ClickHouse queries
@mcp.tool()
def device_healthy(device: str, startTime: str, endTime: str, num: int, unit: str) -> str:
    """设备的健康状态评估

    :param device: 设备名或诊断单id (如：磨煤机D, 1233323).
    :param startTime: 开始日期, 时间格式：%Y-%m-%dT%H:%M:%S+08:00.
    :param endTime: 结束时间, 时间格式：%Y-%m-%dT%H:%M:%S+08:00.
    :param num: 时间的数字
    :param unit: 时间的单位(如：day, week, month)
    Returns:
        类型和参数的字典
    """
    logging.info("device_healthy: {device}, {startTime}, {endTime}, {num}, {unit}")
    url = os.getenv("SERVER_URL")
    response = requests.post(url=url, json={}, headers={"Content-Type": "application/json"})
    logging.info(f"response.status_code: {response.status_code}")
    if response.status_code == 200:
        logging.info(f"response: {response.text}")
        return "发送成功"
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
