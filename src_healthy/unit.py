"""
MCP Server - 机组健康度评估
基于 FastAPI + SSE 传输协议
封装 Java 后端的 HTTP 接口，作为 MCP 工具供大模型调用。

工具列表：
  - unit_healthy       : 一次性获取诊断单 + 故障模式推导图 + 测点实时值（不含 RAG）
  - select_incidents   : 查询机组诊断单列表
  - graph_show         : 获取故障模式推导图
  - tags_realtime      : 获取测点实时值
  - device_rag         : RAG 知识检索
"""

import os
import sys
import logging
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager
from dotenv import load_dotenv

import httpx
from fastapi import FastAPI, Request
from mcp.server.fastmcp import FastMCP
from mcp.server.sse import SseServerTransport

# 加载环境变量
load_dotenv()

# 配置日志
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ========== 配置区域 ==========
JAVA_BASE_URL = os.getenv("JAVA_BASE_URL", "http://192.168.0.44:28080")
MCP_HOST = os.getenv("MCP_HOST", "0.0.0.0")
MCP_PORT = int(os.getenv("MCP_PORT", "8002"))
# ==============================


# ========== API 客户端 (异步封装) ==========

class JavaBackendClient:
    """Java 后端 HTTP 接口客户端"""

    def __init__(self):
        self.base_url = JAVA_BASE_URL
        self.client = None

    def init_client(self):
        """初始化异步客户端"""
        if self.client is None:
            self.client = httpx.AsyncClient(timeout=60.0)

    async def _post(self, path: str, payload: Any) -> str:
        """向 Java 后端发送 POST 请求，返回响应文本。"""
        url = f"{self.base_url}{path}"
        try:
            logger.info(f"POST 请求发送至: {url}, 参数: {payload}")
            resp = await self.client.post(url, json=payload)
            resp.raise_for_status()
            return resp.text
        except httpx.HTTPStatusError as e:
            logger.error(f"后端接口返回错误码 {e.response.status_code}: {e.response.text}")
            return f"错误：后端接口请求失败，状态码：{e.response.status_code}"
        except Exception as e:
            logger.error(f"网络异常或请求超时: {str(e)}")
            return f"错误：无法连接到后端服务或请求超时: {str(e)}"

    async def _get(self, path: str, params: Dict[str, Any]) -> str:
        """向 Java 后端发送 GET 请求，返回响应文本。"""
        url = f"{self.base_url}{path}"
        try:
            logger.info(f"GET 请求发送至: {url}, 参数: {params}")
            resp = await self.client.get(url, params=params)
            resp.raise_for_status()
            return resp.text
        except httpx.HTTPStatusError as e:
            logger.error(f"后端接口返回错误码 {e.response.status_code}: {e.response.text}")
            return f"错误：后端接口请求失败，状态码：{e.response.status_code}"
        except Exception as e:
            logger.error(f"网络异常或请求超时: {str(e)}")
            return f"错误：无法连接到后端服务或请求超时: {str(e)}"

    async def close(self):
        """关闭客户端连接池"""
        if self.client:
            await self.client.aclose()
            self.client = None


# 创建全局唯一的客户端实例
backend_client = JavaBackendClient()


# 使用现代的 lifespan 管理 FastAPI 和 HTTP 客户端的生命周期
@asynccontextmanager
async def lifespan(app: FastAPI):
    # 启动时初始化连接池
    backend_client.init_client()
    yield
    # 关闭时释放连接池
    await backend_client.close()


# 初始化 FastAPI 实例
app = FastAPI(title="机组健康度评估 API", lifespan=lifespan)

# 初始化 MCP 实例
mcp = FastMCP("unit-healthy")


# ========== MCP 工具定义 ==========

@mcp.tool()
async def unit_healthy(
    unit_name: str,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    num: Optional[str] = None,
    time_unit: Optional[str] = None,
    closed: Optional[bool] = None,
) -> str:
    """
    机组健康度数据获取（若用户不指定，不调用该工具）。
    一次性返回：诊断单信息 + 故障模式推导图 + 测点实时值+调 RAG 的场景。

    Args:
        unit_name: 机组名称（必填），如 "京燃"
        start_time: 开始时间（可选），如 "2024-01-01T00:00:00+08:00"
        end_time: 结束时间（可选），如 "2024-01-07T23:59:59+08:00"
        num: 时间跨度数值（可选），与 time_unit 配合使用，如 "7"
        time_unit: 时间单位（可选），可选值：day/week/month/year
        closed: 是否已关闭的诊断单（可选）
    """
    payload = {"unitName": unit_name}
    if start_time: payload["startTime"] = start_time
    if end_time: payload["endTime"] = end_time
    if num: payload["num"] = num
    if time_unit: payload["timeUnit"] = time_unit
    if closed is not None: payload["closed"] = closed

    return await backend_client._post("/ai/unit/healthy", payload)


@mcp.tool()
async def select_incidents(
    unit_name: str,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    num: Optional[str] = None,
    time_unit: Optional[str] = None,
    closed: Optional[bool] = None,
) -> str:
    """
    查询机组诊断单列表。
    根据机组名模糊匹配机组，返回诊断单信息（含 incidentId 等）。

    Args:
        unit_name: 机组名称（必填），如 "京燃"
        start_time: 开始时间（可选）
        end_time: 结束时间（可选）
        num: 时间跨度数值（可选）
        time_unit: 时间单位（可选），day/week/month/year
        closed: 是否已关闭（可选）
    """
    payload = {"unitName": unit_name}
    if start_time: payload["startTime"] = start_time
    if end_time: payload["endTime"] = end_time
    if num: payload["num"] = num
    if time_unit: payload["timeUnit"] = time_unit
    if closed is not None: payload["closed"] = closed

    return await backend_client._post("/ai/unit/selectIncidents", payload)


@mcp.tool()
async def graph_show(incident_ids: List[int]) -> str:
    """
    获取故障模式推导图（故障模式 → 特征 → 测点的层级关系）。

    Args:
        incident_ids: 诊断单 ID 列表，如 [123, 456]
    """
    payload = [{"incidentId": iid} for iid in incident_ids]
    return await backend_client._post("/ai/device/graph/show", payload)


@mcp.tool()
async def tags_realtime(incident_ids: List[int]) -> str:
    """
    获取测点实时值（含测点名称、单位、严重度等级、实际测点数据）。

    Args:
        incident_ids: 诊断单 ID 列表，如 [123, 456]
    """
    payload = [{"incidentId": iid} for iid in incident_ids]
    return await backend_client._post("/ai/device/tagsRealTime", payload)


@mcp.tool()
async def device_rag(tag_name: str) -> str:
    """
    RAG 知识检索。根据测点名称检索相关知识，返回历史知识和处理建议。

    Args:
        tag_name: 测点名称，多个用逗号分隔，内容精简在 50 字以内
    """
    return await backend_client._get("/ai/device/rag", {"tagName": tag_name})


@mcp.tool()
async def get_alarm_list(
    unit_id: Optional[int] = None,
    tag_name: Optional[str] = None,
    tag_source_name: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    asset_number: Optional[int] = None,
    data_type: Optional[str] = None,
    current_status_name: Optional[str] = None,
    tag_id: Optional[int] = None,
    monitor_point_id: Optional[int] = None,
    closed: Optional[bool] = None,
) -> str:
    """
    查询测点报警单列表。支持多维度筛选告警信息。

    Args:
        unit_id: 机组ID（可选），查询特定机组下的所有告警
        tag_name: 测点名称（可选），用于模糊匹配
        tag_source_name: 测点来源名称（可选）
        start_time: 开始时间（可选），查询 firsttouchtime >= 该时间的告警
        end_time: 结束时间（可选），查询 lasttouchtime <= 该时间的告警
        asset_number: 设备编号（可选）
        data_type: 数据类型（可选），如 "告警"、"缺陷" 等
        current_status_name: 当前状态名称（可选），如 "待处理"、"处理中"、"已关闭"
        tag_id: 测点ID（可选），精确查询某个测点的告警
        monitor_point_id: 监测点ID（可选）
        closed: 是否已关闭（可选，默认false），true表示查询已关闭的告警
    """
    payload = {}
    if unit_id is not None: payload["unitId"] = unit_id
    if tag_name: payload["tagName"] = tag_name
    if tag_source_name: payload["tagSourceName"] = tag_source_name
    if start_time: payload["startTime"] = start_time
    if end_time: payload["endTime"] = end_time
    if asset_number is not None: payload["assetNumber"] = asset_number
    if data_type: payload["dataType"] = data_type
    if current_status_name: payload["currentStatusName"] = current_status_name
    if tag_id is not None: payload["tagId"] = tag_id
    if monitor_point_id is not None: payload["monitorPointId"] = monitor_point_id
    if closed is not None: payload["closed"] = closed

    return await backend_client._post("/ai/unit/getAlarmList", payload)


@mcp.tool()
async def get_system_incident_list(
    unit_id: Optional[int] = None,
    system_id: Optional[int] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    current_status: Optional[str] = None,
    closed: Optional[bool] = None,
) -> str:
    """
    查询系统诊断单列表。支持多维度筛选系统级诊断单信息。

    Args:
        unit_id: 机组ID（可选），查询特定机组下的系统诊断单
        system_id: 系统ID（可选），查询特定系统的诊断单
        start_time: 开始时间（可选），与 end_time 配合使用
        end_time: 结束时间（可选），与 start_time 配合使用
        current_status: 当前状态（可选），如 "待处理"、"处理中"、"已关闭"
        closed: 是否已关闭（可选，默认false），true表示查询已关闭的诊断单
    """
    payload = {}
    if unit_id is not None: payload["unitId"] = unit_id
    if system_id is not None: payload["systemId"] = system_id
    if start_time: payload["startTime"] = start_time
    if end_time: payload["endTime"] = end_time
    if current_status: payload["currentStatus"] = current_status
    if closed is not None: payload["closed"] = closed

    return await backend_client._post("/ai/unit/getSystemIncidentList", payload)


@mcp.tool()
async def get_sub_system_incident_list(
    unit_id: Optional[int] = None,
    sub_system_id: Optional[int] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    current_status: Optional[str] = None,
    closed: Optional[bool] = None,
) -> str:
    """
    查询子系统诊断单列表。支持多维度筛选子系统级诊断单信息。

    Args:
        unit_id: 机组ID（可选），查询特定机组下的子系统诊断单
        sub_system_id: 子系统ID（可选），查询特定子系统的诊断单
        start_time: 开始时间（可选），与 end_time 配合使用
        end_time: 结束时间（可选），与 start_time 配合使用
        current_status: 当前状态（可选），如 "待处理"、"处理中"、"已关闭"
        closed: 是否已关闭（可选，默认false），true表示查询已关闭的诊断单
    """
    payload = {}
    if unit_id is not None: payload["unitId"] = unit_id
    if sub_system_id is not None: payload["subSystemId"] = sub_system_id
    if start_time: payload["startTime"] = start_time
    if end_time: payload["endTime"] = end_time
    if current_status: payload["currentStatus"] = current_status
    if closed is not None: payload["closed"] = closed

    return await backend_client._post("/ai/unit/getSubSystemIncidentList", payload)


# ========== MCP 标准 SSE 传输层配置 ==========

# 初始化标准 MCP SSE 传输器，定义客户端发布/接收消息的基准端点为 /messages
transport = SseServerTransport("/messages")


@app.get("/sse")
async def handle_sse(request: Request):
    """
    处理 MCP 客户端发起的 SSE 长连接握手。
    """
    async with transport.connect_sse(
        request.scope, request.receive, request._send
    ) as streams:
        await mcp._mcp_server.run(
            streams[0], streams[1], mcp._mcp_server.create_initialization_options()
        )


# 使用 Mount 直接挂载 handle_post_message 作为 ASGI 子应用（MCP 官方推荐方式）
# 避免 FastAPI 路由二次发送 ASGI 响应导致 RuntimeError
app.mount("/messages", transport.handle_post_message)


# ========== 服务入口控制 ==========

if __name__ == "__main__":
    import uvicorn

    if "--stdio" in sys.argv:
        mcp.run(transport="stdio")
    else:
        logger.info(f"正在启动机组健康度 MCP 模块 [SSE 协议] 监听: http://{MCP_HOST}:{MCP_PORT}")
        logger.info(f"提示: 请配置 MCP 客户端连接: http://{MCP_HOST}:{MCP_PORT}/sse")
        uvicorn.run(app, host=MCP_HOST, port=MCP_PORT, reload=False)