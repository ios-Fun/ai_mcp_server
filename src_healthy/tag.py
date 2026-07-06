"""
MCP Server - 测点信息查询
基于 FastAPI + SSE 传输协议
封装 Java 后端的 HTTP 接口,作为 MCP 工具供大模型调用。

工具列表:
  - search_tags          : 通过测点ID/编码/源标签点名准确查找或名称模糊查询测点信息
  - get_tag_paths        : 通过测点ID/编码/源标签点名准确查找测点挂载路径
  - get_tag_values       : 通过测点ID/编码/源标签点名准确查找指定时间段的测点实际值、估计值、严重度
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
JAVA_BASE_URL = os.getenv("JAVA_BASE_URL", "http://localhost:28080")
MCP_HOST = os.getenv("MCP_HOST", "0.0.0.0")
MCP_PORT = int(os.getenv("MCP_PORT", "8003"))
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
        """向 Java 后端发送 POST 请求,返回响应文本。"""
        url = f"{self.base_url}{path}"
        try:
            logger.info(f"POST 请求发送至: {url}, 参数: {payload}")
            resp = await self.client.post(url, json=payload)
            resp.raise_for_status()
            return resp.text
        except httpx.HTTPStatusError as e:
            logger.error(f"后端接口返回错误码 {e.response.status_code}: {e.response.text}")
            return f"错误:后端接口请求失败,状态码:{e.response.status_code}"
        except Exception as e:
            logger.error(f"网络异常或请求超时: {str(e)}")
            return f"错误:无法连接到后端服务或请求超时: {str(e)}"

    async def _get(self, path: str, params: Dict[str, Any]) -> str:
        """向 Java 后端发送 GET 请求,返回响应文本。"""
        url = f"{self.base_url}{path}"
        try:
            logger.info(f"GET 请求发送至: {url}, 参数: {params}")
            resp = await self.client.get(url, params=params)
            resp.raise_for_status()
            return resp.text
        except httpx.HTTPStatusError as e:
            logger.error(f"后端接口返回错误码 {e.response.status_code}: {e.response.text}")
            return f"错误:后端接口请求失败,状态码:{e.response.status_code}"
        except Exception as e:
            logger.error(f"网络异常或请求超时: {str(e)}")
            return f"错误:无法连接到后端服务或请求超时: {str(e)}"

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
app = FastAPI(title="测点信息查询 API", lifespan=lifespan)

# 初始化 MCP 实例
mcp = FastMCP("tag-info")


# ========== MCP 工具定义 ==========

@mcp.tool()
async def search_tags(
    tag_id: Optional[int] = None,
    tag_code: Optional[str] = None,
    src_tag_name: Optional[str] = None,
    tag_name: Optional[str] = None,
) -> str:
    """
    测点信息查询工具。
    支持两种方式:
    1. 精确查询: 通过 tag_id(测点ID)、tag_code(测点编码)、src_tag_name(源标签点名) 三者之一进行精确匹配
    2. 模糊查询: 通过 tag_name(测点名称) 进行模糊匹配
    
    注意: 精确查询的三个参数(tag_id/tag_code/src_tag_name)只需填写一个即可,如果同时提供多个,优先级为: tag_id > tag_code > src_tag_name
    如果使用模糊查询(tag_name),则不能同时使用精确查询参数。

    Args:
        tag_id: 测点ID(可选),精确匹配
        tag_code: 测点编码(可选),精确匹配
        src_tag_name: 源标签点名(可选),精确匹配
        tag_name: 测点名称(可选),模糊匹配
    
    Returns:
        测点信息列表,包含测点ID、名称、编码、源标签点名、单位、描述等信息
    """
    # TODO: 实现测点查询逻辑
    # 需要调用 Java 后端接口,例如: /ai/tag/search
    # 根据传入的参数构建请求体并调用后端接口
    
    payload = {}
    if tag_id is not None:
        payload["tagId"] = tag_id
    if tag_code:
        payload["tagCode"] = tag_code
    if src_tag_name:
        payload["srcTagName"] = src_tag_name
    if tag_name:
        payload["tagName"] = tag_name
    
    if not payload:
        return "错误: 请至少提供一个查询参数(tag_id/tag_code/src_tag_name/tag_name)"
    
    # 示例: 调用后端接口(待实现)
    # return await backend_client._post("/ai/tag/search", payload)
    
    return f"TODO: 实现测点查询功能\n查询参数: {payload}\n需要补充 Java 后端接口调用"


@mcp.tool()
async def get_tag_paths(
    tag_id: Optional[int] = None,
    tag_code: Optional[str] = None,
    src_tag_name: Optional[str] = None,
) -> str:
    """
    测点挂载路径查询工具。
    通过测点ID、编码或源标签点名精确查找测点的挂载路径。
    一个测点可能挂载在多处,因此会返回多条路径信息。
    
    三个参数(tag_id/tag_code/src_tag_name)只需填写一个即可,优先级为: tag_id > tag_code > src_tag_name

    Args:
        tag_id: 测点ID(可选),精确匹配
        tag_code: 测点编码(可选),精确匹配
        src_tag_name: 源标签点名(可选),精确匹配
    
    Returns:
        测点挂载路径列表,每条路径包含完整的层级关系(如: 机组->系统->子系统->设备->测点)
    """
    # TODO: 实现测点路径查询逻辑
    # 需要调用 Java 后端接口,例如: /ai/tag/paths
    # 根据传入的参数构建请求体并调用后端接口
    
    payload = {}
    if tag_id is not None:
        payload["tagId"] = tag_id
    elif tag_code:
        payload["tagCode"] = tag_code
    elif src_tag_name:
        payload["srcTagName"] = src_tag_name
    else:
        return "错误: 请至少提供一个查询参数(tag_id/tag_code/src_tag_name)"
    
    # 示例: 调用后端接口(待实现)
    # return await backend_client._post("/ai/tag/paths", payload)
    
    return f"TODO: 实现测点路径查询功能\n查询参数: {payload}\n需要补充 Java 后端接口调用"


@mcp.tool()
async def get_tag_values(
    tag_id: Optional[int] = None,
    tag_code: Optional[str] = None,
    src_tag_name: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
) -> str:
    """
    测点历史数据查询工具。
    通过测点ID、编码或源标签点名精确查找指定时间段内的测点数据,包括:
    - 实际值(actual_value)
    - 估计值(estimated_value)
    - 严重度(severity)
    
    如果不传时间参数,默认查询最近6小时到现在的数据。
    三个标识参数(tag_id/tag_code/src_tag_name)只需填写一个即可,优先级为: tag_id > tag_code > src_tag_name

    Args:
        tag_id: 测点ID(可选),精确匹配
        tag_code: 测点编码(可选),精确匹配
        src_tag_name: 源标签点名(可选),精确匹配
        start_time: 开始时间(可选),格式如 "2024-01-01T00:00:00+08:00",不传则默认为6小时前
        end_time: 结束时间(可选),格式如 "2024-01-07T23:59:59+08:00",不传则默认为当前时间
    
    Returns:
        测点历史数据列表,包含时间戳、实际值、估计值、严重度等信息
    """
    # TODO: 实现测点历史数据查询逻辑
    # 需要调用 Java 后端接口,例如: /ai/tag/values
    # 根据传入的参数构建请求体并调用后端接口
    
    payload = {}
    if tag_id is not None:
        payload["tagId"] = tag_id
    elif tag_code:
        payload["tagCode"] = tag_code
    elif src_tag_name:
        payload["srcTagName"] = src_tag_name
    else:
        return "错误: 请至少提供一个查询参数(tag_id/tag_code/src_tag_name)"
    
    if start_time:
        payload["startTime"] = start_time
    if end_time:
        payload["endTime"] = end_time
    
    # 如果没有传时间,默认查询最近6小时
    if not start_time and not end_time:
        from datetime import datetime, timedelta, timezone
        now = datetime.now(timezone.utc)
        six_hours_ago = now - timedelta(hours=6)
        payload["startTime"] = six_hours_ago.strftime("%Y-%m-%dT%H:%M:%S+00:00")
        payload["endTime"] = now.strftime("%Y-%m-%dT%H:%M:%S+00:00")
    
    # 示例: 调用后端接口(待实现)
    # return await backend_client._post("/ai/tag/values", payload)
    
    return f"TODO: 实现测点历史数据查询功能\n查询参数: {payload}\n需要补充 Java 后端接口调用"


# ========== MCP 标准 SSE 传输层配置 ==========

# 初始化标准 MCP SSE 传输器,定义客户端发布/接收消息的基准端点为 /messages
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


# 使用 Mount 直接挂载 handle_post_message 作为 ASGI 子应用(MCP 官方推荐方式)
# 避免 FastAPI 路由二次发送 ASGI 响应导致 RuntimeError
app.mount("/messages", transport.handle_post_message)


# ========== 服务入口控制 ==========

if __name__ == "__main__":
    import uvicorn

    if "--stdio" in sys.argv:
        mcp.run(transport="stdio")
    else:
        logger.info(f"正在启动测点信息查询 MCP 模块 [SSE 协议] 监听: http://{MCP_HOST}:{MCP_PORT}")
        logger.info(f"提示: 请配置 MCP 客户端连接: http://{MCP_HOST}:{MCP_PORT}/sse")
        uvicorn.run(app, host=MCP_HOST, port=MCP_PORT, reload=False)
