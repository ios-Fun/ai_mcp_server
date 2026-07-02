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
def cg_device_healthy(orginal: str, thread_id: str = "") -> str:
    """设备的健康状态评估

    :param orginal: 原文
    :param thread_id: 线程id
    Returns:
        字符串
    """
    logging.info(f"cg_device_healthy: {orginal}")
    
    # rasa
    rasa_url = os.getenv("RASA_URL")
    rasa_data = {
        "sender":"sender002", "message":orginal
    }
    rasa_response = requests.post(url=rasa_url, json=rasa_data, headers={"Content-Type": "application/json"})
    logging.info(f"rasa_response: {rasa_response.text}")
    
    rasa_obj = rasa_response.text
    data = json.loads(rasa_obj)

    logging.info(f"first: {data[0]}")
    first_text = data[0]["text"]
    logging.info(f"first_text: {first_text}")
    
    second_custom = data[1]["custom"]
    logging.info(f"second_custom: {second_custom}")
    
    java_url = os.getenv("SERVER_URL")+"/device/healthy/v3"
    logging.info(f"java_url: {java_url}")
    response = requests.post(url=java_url, json=second_custom, headers={"Content-Type": "application/json"})
    logging.info(f"response.status_code: {response.status_code}")
    if response.status_code == 200:
        logging.info(f"cg_device_healthy response: {response.text}")
        return response.text
    else:
        return "发送失败"

@mcp.tool()
def cg_graphshow(cached_defectIds: list, thread_id: str = "") -> str:
    """显示故障模式

    :param cached_defectIds: 诊断单的信息
    :param thread_id: 线程id
    Returns:
        字符串
    """
    logging.info(f"cg_graphshow: {cached_defectIds}")
    # java_url = os.getenv("SERVER_URL").join("/device/grpah/show")
    java_url = os.getenv("SERVER_URL")+"/device/graph/show"

    response = requests.post(url=java_url, json=cached_defectIds, headers={"Content-Type": "application/json"})
    logging.info(f"response.status_code: {response.status_code}")
    if response.status_code == 200:
        logging.info(f"cg_graphshow response: {response.text}")
        return response.text
    else:
        return "发送失败"

@mcp.tool()
def cg_deviceRag(cached_defectIds: list, thread_id: str = "") -> str:
    """查询rag信息

    :param cached_defectIds: 诊断单的信息
    :param thread_id: 线程id    
    Returns:
        字符串
    """
    logging.info(f"cg_deviceRag: {cached_defectIds}")
    java_url = os.getenv("SERVER_URL")+"/device/rag/v2"

    response = requests.post(url=java_url, json=cached_defectIds, headers={"Content-Type": "application/json"})
    logging.info(f"response.status_code: {response.status_code}")
    if response.status_code == 200:
        logging.info(f"cg_deviceRag response: {response.text}")
        return response.text
    else:
        return "发送失败"
        
        
@mcp.tool()
def deviceRag(ragInfo: str, thread_id: str = "") -> str:
    """查询rag信息

    :param ragInfo: 待检索关键信息
    :param thread_id: 线程id    
    Returns:
        字符串
    """
    logging.info(f"deviceRag: {ragInfo}")
    java_url = os.getenv("SERVER_URL")+"/device/rag"

    response = requests.post(url=java_url, json=ragInfo, headers={"Content-Type": "application/json"})
    logging.info(f"response.status_code: {response.status_code}")
    if response.status_code == 200:
        logging.info(f"cg_deviceRag response: {response.text}")
        return response.text
    else:
        return "发送失败"

@mcp.tool()
def cg_tagsRealtimeValues(cached_defectIds: list, thread_id: str = "") -> str:
    """显示测点实际值

    :param cached_defectIds: 诊断单的信息
    :param thread_id: 线程id    
    Returns:
        字符串
    """
    logging.info(f"cg_tagsRealtimeValues: {cached_defectIds}")
    java_url = os.getenv("SERVER_URL")+"/device/tagsRealTime"
    response = requests.post(url=java_url, json=cached_defectIds, headers={"Content-Type": "application/json"})
    logging.info(f"response.status_code: {response.status_code}")
    if response.status_code == 200:
        logging.info(f"cg_tagsRealtimeValues response: {response.text}")
        return response.text
    else:
        return "发送失败"

@mcp.tool()
def cg_tagsInfoList(cached_defectIds: list, thread_id: str = "") -> str:
    """获取测点信息

    :param cached_defectIds: 诊断单的信息
    :param thread_id: 线程id    
    Returns:
        字典信息
    """
    logging.info(f"cg_tagsInfoList: {cached_defectIds}")
    java_url = os.getenv("SERVER_URL")+"/device/tagsInfoList"
    response = requests.post(url=java_url, json=cached_defectIds, headers={"Content-Type": "application/json"})
    logging.info(f"response.status_code: {response.status_code}")
    if response.status_code == 200:
        logging.info(f"cg_tagsInfoList response: {response.text}")
        return response.text
    else:
        return "发送失败"

@mcp.tool()
def cg_tagTrend(cached_TagsTrendPara: list, thread_id: str = "") -> str:
    """显示测点实际值

    :param cached_TagsTrendPara: 测点的信息
    :param thread_id: 线程id    
    Returns:
        字符串
    """
    logging.info(f"cg_tagTrend: {cached_TagsTrendPara}")
    java_url = os.getenv("SERVER_URL")+"/device/tagsTrend"
    response = requests.post(url=java_url, json=cached_TagsTrendPara, headers={"Content-Type": "application/json"})
    logging.info(f"response.status_code: {response.status_code}")
    if response.status_code == 200:
        logging.info(f"cg_tagTrend response: {response.text}")
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
