# server.py
from fastapi import FastAPI, Request
from sse_starlette.sse import EventSourceResponse
from mcp.server.fastmcp import FastMCP
from mcp.server.sse import SseServerTransport
from starlette.routing import Mount, Route
from starlette.applications import Starlette
from typing import Optional, List, Dict, Any
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
server_url = os.getenv("SERVER_URL")

# 配置日志
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

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

#============机组相关MCP=================================================================
"""
工具列表：
  - unit_healthy       : 一次性获取诊断单 + 故障模式推导图 + 测点实时值（不含 RAG）
  - select_incidents   : 查询机组诊断单列表
  - graph_show         : 获取故障模式推导图
  - tags_realtime      : 获取测点实时值
  - device_rag         : RAG 知识检索
"""

@mcp.tool()
def unit_healthy(
        unit_name: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        num: Optional[str] = None,
        time_unit: Optional[str] = None,
        closed: Optional[bool] = None,
) -> str:
    """
    【特殊需求专用】机组健康度数据获取。

    ⚠️ 注意：此工具仅在用户明确要求执行"机组健康度简单分析"或"unit_healthy"时调用，不要主动使用。
    一次性返回：诊断单信息 + 故障模式推导图 + 测点实时值。

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
    url = f"{server_url}/unit/healthy"

    try:
        logger.info(f"POST 请求发送至: {url}, 参数: {payload}")
        resp = requests.post(url, json=payload, headers={"Content-Type": "application/json"})
        resp.raise_for_status()
        return resp.text
    except requests.exceptions.HTTPError as e:
        return f"错误：后端接口请求失败，状态码：{e.response.status_code}"
    except requests.exceptions.RequestException as e:
        return f"错误：请求异常: {str(e)}"

@mcp.tool()
def unit_select_incidents(
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

    url = f"{server_url}/unit/selectIncidents"
    try:
        logger.info(f"POST 请求发送至: {url}, 参数: {payload}")
        resp = requests.post(url, json=payload, headers={"Content-Type": "application/json"})
        resp.raise_for_status()
        return resp.text
    except requests.exceptions.HTTPError as e:
        return f"错误：后端接口请求失败，状态码：{e.response.status_code}"
    except requests.exceptions.RequestException as e:
        return f"错误：请求异常: {str(e)}"

@mcp.tool()
def unit_graph_show(incident_ids: List[int]) -> str:
    """
    获取故障模式推导图（故障模式 → 特征 → 测点的层级关系）。

    Args:
        incident_ids: 诊断单 ID 列表，如 [123, 456]
    """
    payload = [{"incidentId": iid} for iid in incident_ids]

    url = f"{server_url}/device/graph/show"
    try:
        logger.info(f"POST 请求发送至: {url}, 参数: {payload}")
        resp = requests.post(url, json=payload, headers={"Content-Type": "application/json"})
        resp.raise_for_status()
        return resp.text
    except requests.exceptions.HTTPError as e:
        return f"错误：后端接口请求失败，状态码：{e.response.status_code}"
    except requests.exceptions.RequestException as e:
        return f"错误：请求异常: {str(e)}"

@mcp.tool()
def unit_tags_realtime(incident_ids: List[int]) -> str:
    """
    获取测点实时值（含测点名称、单位、严重度等级、实际测点数据）。

    Args:
        incident_ids: 诊断单 ID 列表，如 [123, 456]
    """
    payload = [{"incidentId": iid} for iid in incident_ids]

    url = f"{server_url}/device/tagsRealTime"
    try:
        logger.info(f"POST 请求发送至: {url}, 参数: {payload}")
        resp = requests.post(url, json=payload, headers={"Content-Type": "application/json"})
        resp.raise_for_status()
        return resp.text
    except requests.exceptions.HTTPError as e:
        return f"错误：后端接口请求失败，状态码：{e.response.status_code}"
    except requests.exceptions.RequestException as e:
        return f"错误：请求异常: {str(e)}"

@mcp.tool()
def unit_device_rag(tag_name: str) -> str:
    """
    RAG 知识检索。根据测点名称检索相关知识，返回历史知识和处理建议。
    Args:
        tag_name: 测点名称，多个用逗号分隔
    """
    url = f"{server_url}/device/rag"
    try:
        logger.info(f"POST 请求发送至: {url}, 参数: tagName: {tag_name}")
        resp = requests.post(url, params={"tagName": tag_name})
        resp.raise_for_status()
        return resp.text
    except requests.exceptions.HTTPError as e:
        return f"错误：后端接口请求失败，状态码：{e.response.status_code}"
    except requests.exceptions.RequestException as e:
        return f"错误：请求异常: {str(e)}"

@mcp.tool()
def get_alarm_list(
        unit_id: Optional[int] = None,
        tag_code: Optional[str] = None,
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
        tag_code: 测点编码（可选）
        tag_source_name: 测点源标签点名（可选）
        start_time: 开始时间（可选），查询 firsttouchtime >= 该时间的告警
        end_time: 结束时间（可选），查询 lasttouchtime <= 该时间的告警
        asset_number: 设备编号（可选）
        data_type: 数据类型（可选），如 "告警"、"缺陷" 等
        current_status_name: 当前状态名称（可选），如 "新报警单"、"已关闭"等
        tag_id: 测点ID（可选），精确查询某个测点的告警
        monitor_point_id: 监测点ID（可选）
        closed: 是否已关闭（可选，默认false），true表示查询已关闭的告警
    """
    payload = {}
    if unit_id is not None: payload["unitId"] = unit_id
    if tag_code: payload["tagName"] = tag_code
    if tag_source_name: payload["tagSourceName"] = tag_source_name
    if start_time: payload["startTime"] = start_time
    if end_time: payload["endTime"] = end_time
    if asset_number is not None: payload["assetNumber"] = asset_number
    if data_type: payload["dataType"] = data_type
    if current_status_name: payload["currentStatusName"] = current_status_name
    if tag_id is not None: payload["tagId"] = tag_id
    if monitor_point_id is not None: payload["monitorPointId"] = monitor_point_id
    if closed is not None: payload["closed"] = closed

    url = f"{server_url}/unit/getAlarmList"
    try:
        logger.info(f"POST 请求发送至: {url}, 参数: {payload}")
        resp = requests.post(url, json=payload, headers={"Content-Type": "application/json"})
        resp.raise_for_status()
        return resp.text
    except requests.exceptions.HTTPError as e:
        return f"错误：后端接口请求失败，状态码：{e.response.status_code}"
    except requests.exceptions.RequestException as e:
        return f"错误：请求异常: {str(e)}"

@mcp.tool()
def get_system_incident_list(
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

    url = f"{server_url}/unit/getSystemIncidentList"
    try:
        logger.info(f"POST 请求发送至: {url}, 参数: {payload}")
        resp = requests.post(url, json=payload, headers={"Content-Type": "application/json"})
        resp.raise_for_status()
        return resp.text
    except requests.exceptions.HTTPError as e:
        return f"错误：后端接口请求失败，状态码：{e.response.status_code}"
    except requests.exceptions.RequestException as e:
        return f"错误：请求异常: {str(e)}"

@mcp.tool()
def get_sub_system_incident_list(
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

    url = f"{server_url}/unit/getSubSystemIncidentList"
    try:
        logger.info(f"POST 请求发送至: {url}, 参数: {payload}")
        resp = requests.post(url, json=payload, headers={"Content-Type": "application/json"})
        resp.raise_for_status()
        return resp.text
    except requests.exceptions.HTTPError as e:
        return f"错误：后端接口请求失败，状态码：{e.response.status_code}"
    except requests.exceptions.RequestException as e:
        return f"错误：请求异常: {str(e)}"

#============测点相关MCP==================================================================
"""
工具列表:
  - search_tags          : 通过测点ID/编码/源标签点名准确查找或名称模糊查询测点信息
  - get_tag_paths        : 通过测点ID/编码/源标签点名准确查找测点挂载路径
  - get_tag_values       : 通过测点ID/编码/源标签点名准确查找指定时间段的测点实际值、估计值、严重度,(默认1小时间隔)
"""

@mcp.tool()
def search_tags(
        tag_id: Optional[int] = None,
        tag_code: Optional[str] = None,
        src_tag_name: Optional[str] = None,
        name: Optional[str] = None,
) -> str:
    """
    测点信息查询工具。
    支持两种方式:
    1. 精确查询: 通过 tag_id(测点ID)、tag_code(测点编码)、src_tag_name(源标签点名) 三者之一进行精确匹配
    2. 模糊查询: 通过 tag_name(测点名称) 进行模糊匹配

    注意: 精确查询的三个参数(tag_id/tag_code/src_tag_name)只需填写一个即可,如果同时提供多个,优先级为: tag_id > tag_code > src_tag_name
    如果使用模糊查询(name),则不能同时使用精确查询参数。

    Args:
        tag_id: 测点ID(可选),精确匹配
        tag_code: 测点编码(可选),精确匹配
        src_tag_name: 源标签点名(可选),精确匹配
        name: 测点名称(可选),模糊匹配

    Returns:
        测点信息列表,包含测点ID、名称、编码、源标签点名、单位、描述等信息
    """

    payload = {}
    if tag_id is not None:
        payload["tagId"] = tag_id
    if tag_code:
        payload["tagName"] = tag_code
    if src_tag_name:
        payload["srcTagName"] = src_tag_name
    if name:
        payload["name"] = name
    if not payload:
        return "错误: 请至少提供一个查询参数(tag_id/tag_code/src_tag_name/name)"

    url = f"{server_url}/tag/getTagInfos"
    try:
        logger.info(f"POST 请求发送至: {url}, 参数: {payload}")
        resp = requests.post(url, params=payload)
        resp.raise_for_status()
        return resp.text
    except requests.exceptions.HTTPError as e:
        return f"错误：后端接口请求失败，状态码：{e.response.status_code}"
    except requests.exceptions.RequestException as e:
        return f"错误：请求异常: {str(e)}"

@mcp.tool()
def get_tag_paths(
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

    payload = {}
    if tag_id is not None:
        payload["tagId"] = tag_id
    elif tag_code:
        payload["tagName"] = tag_code
    elif src_tag_name:
        payload["srcTagName"] = src_tag_name
    else:
        return "错误: 请至少提供一个查询参数(tag_id/tag_code/src_tag_name)"

    url = f"{server_url}/tag/getTagPaths"
    try:
        logger.info(f"POST 请求发送至: {url}, 参数: {payload}")
        resp = requests.post(url, params=payload)
        resp.raise_for_status()
        return resp.text
    except requests.exceptions.HTTPError as e:
        return f"错误：后端接口请求失败，状态码：{e.response.status_code}"
    except requests.exceptions.RequestException as e:
        return f"错误：请求异常: {str(e)}"

@mcp.tool()
def get_tag_values(
        tag_id: Optional[int] = None,
        tag_code: Optional[str] = None,
        src_tag_name: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        type: str = "RealTimeData",
        interval: Optional[int] = 3600
) -> str:
    """
    测点历史数据查询工具。
    通过测点ID、编码或源标签点名精确查找指定时间段内的测点数据type,包括:
    - 实际值(RealTimeData)
    - 估计值(Estimate)
    - 严重度(TagSeverity)

    如果不传时间参数,默认查询最近6小时到现在的数据。
    三个标识参数(tag_id/tag_code/src_tag_name)只需填写一个即可,优先级为: tag_code > tag_id > src_tag_name
    默认查询时间间隔为3600s，也就是1小时，可根据时间段长短设置大的时间间隔返回数据避免过度使用token

    Args:
        tag_id: 测点ID(可选),精确匹配
        tag_code: 测点编码(可选),精确匹配
        src_tag_name: 源标签点名(可选),精确匹配
        start_time: 开始时间(可选),格式如 "2024-01-01T00:00:00+08:00",不传则默认为6小时前
        end_time: 结束时间(可选),格式如 "2024-01-07T23:59:59+08:00",不传则默认为当前时间
        type: 查询类型(必填),默认为实际值(RealTimeData),可选值: RealTimeData,Estimate,TagSeverity,all
        interval: 测点查询时间间隔
    Returns:
        测点历史数据列表,包含时间戳、实际值、估计值、严重度等信息
    """
    payload = {}
    if tag_id is not None:
        payload["tagId"] = tag_id
    elif tag_code:
        payload["tagName"] = tag_code
    elif src_tag_name:
        payload["srcTagName"] = src_tag_name
    else:
        return "错误: 请至少提供一个查询参数(tag_id/tag_code/src_tag_name)"

    if start_time:
        payload["startTime"] = start_time
    if end_time:
        payload["endTime"] = end_time
    payload["type"] = type
    if interval:
        payload["interval"] = interval

    # 如果没有传时间,默认查询最近6小时
    if not start_time and not end_time:
        from datetime import datetime, timedelta, timezone
        now = datetime.now(timezone.utc)
        six_hours_ago = now - timedelta(hours=6)
        payload["startTime"] = six_hours_ago.strftime("%Y-%m-%dT%H:%M:%S+00:00")
        payload["endTime"] = now.strftime("%Y-%m-%dT%H:%M:%S+00:00")

    url = f"{server_url}/tag/tagValues"
    try:
        logger.info(f"POST 请求发送至: {url}, 参数: {payload}")
        resp = requests.post(url, params=payload)
        resp.raise_for_status()
        return resp.text
    except requests.exceptions.HTTPError as e:
        return f"错误：后端接口请求失败，状态码：{e.response.status_code}"
    except requests.exceptions.RequestException as e:
        return f"错误：请求异常: {str(e)}"

#============工具相关MCP==================================================================

@mcp.tool()
def match_for_best(match_string: str) -> str:
    """
    实例模糊匹配工具。
    根据的“用户输入的完整问题”，从所有实例中模糊匹配出相似度最高的前10个实例。
    匹配逻辑基于混合杰卡德相似度（字符级 + 词级 + 基础杰卡德），对实例名称进行相似度计算。

    使用场景：当用户输入的语句不能精确辨识实体时，可先调用此工具将用户发送的整个语句传入进行模糊匹配，
    获取最可能的实例列表后再进行后续操作。

    Args:
        match_string: 用于模糊匹配的字符串，如设备名称、测点名称等

    Returns:
        相似度最高的前10个实例信息列表，每个实例包含 id、name、code、type、similarity 字段
    """
    url = f"{server_url}/common/matchForBest"
    try:
        logger.info(f"POST 请求发送至: {url}, 参数: matchString={match_string}")
        resp = requests.post(url, params={"matchString": match_string})
        resp.raise_for_status()
        return resp.text
    except requests.exceptions.HTTPError as e:
        return f"错误：后端接口请求失败，状态码：{e.response.status_code}"
    except requests.exceptions.RequestException as e:
        return f"错误：请求异常: {str(e)}"


#=========================================================================================

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
