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
server_url1 = os.getenv("SERVER_URL1")

# 配置日志
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Define an MCP tool to execute ClickHouse queries

# 设备的健康状态评估 -- cg_device_healthy
@mcp.tool()
def cg_device_healthy(device: str, startTime: str, endTime: str, thread_id: str) -> str:
    """
    查询设备的健康状态评估报告。根据设备名称获取诊断单、报警统计、
    故障模式推导图、测点实时数据，综合分析后生成健康评估报告。

    适用场景：用户询问某台设备的运行情况、健康状态、故障情况、
    异常情况、是否正常、最近有没有问题、运行是否稳定等。

    参数说明：
        device: 设备名称，如"#1给水泵""凝汽器""引风机A"
        startTime: 开始时间，格式"2024-01-01T00:00:00+08:00"，不传默认6小时前
        endTime: 结束时间，格式"2024-01-07T23:59:59+08:00"，不传默认当前时间
        thread_id: DeerFlow 执行线程 ID
    """
    # logging.info(f"cg_device_healthy: {orginal}")
    #
    # # rasa
    # rasa_url = os.getenv("RASA_URL")
    # rasa_data = {
    #     "sender":"sender002", "message":orginal
    # }
    # rasa_response = requests.post(url=rasa_url, json=rasa_data, headers={"Content-Type": "application/json"})
    # logging.info(f"rasa_response: {rasa_response.text}")
    #
    # rasa_obj = rasa_response.text
    # data = json.loads(rasa_obj)
    #
    # logging.info(f"first: {data[0]}")
    # first_text = data[0]["text"]
    # logging.info(f"first_text: {first_text}")
    #
    # second_custom = data[1]["custom"]
    # logging.info(f"second_custom: {second_custom}")
    second_custom = {}
    second_custom["device"] = device
    second_custom["startTime"] = startTime
    second_custom["endTime"] = endTime
    logging.info(f"second_custom: {second_custom}")

    java_url = os.getenv("SERVER_URL") + "/device/healthy/v3"
    logging.info(f"java_url: {java_url}")
    response = requests.post(url=java_url, json=second_custom, headers={"Content-Type": "application/json"})
    logging.info(f"response.status_code: {response.status_code}")
    if response.status_code == 200:
        logging.info(f"cg_device_healthy response: {response.text}")
        if thread_id:
            try:
                resp_data = json.loads(response.text)
                defect_ids = resp_data.get("cached_defectIds")
                if defect_ids is not None:
                    redis_key = f"{thread_id}_cached_defectIds"
                    memoryRedis.set_cache(redis_key, defect_ids)
                    logger.info(f"cached defectIds -> {redis_key}: {defect_ids}")
            except (json.JSONDecodeError, TypeError, ValueError) as e:
                logger.info(f"cg_device_healthy cache write skipped: {e}")
            return response.text
    else:
        return "发送失败"

@mcp.tool()
def cg_graphshow(thread_id: str = "",
                 defect_ids: Optional[List] = None) -> str:
    """
    获取设备故障模式推导图（知识图谱）。

    从诊断单出发，展示该故障模式的完整推导链路。
    用于辅助分析故障根因、判断异常是否与故障模式吻合、
    以及生成运维建议。

    适用场景：
    - 用户询问某个故障模式的具体触发条件是什么
    - 需要分析故障的发展路径和因果关系
    - 生成设备健康评估报告时需要故障知识图谱支撑

    参数：
        thread_id: DeerFlow 执行线程 ID，用于关联当前诊断上下文
        defect_ids: 故障ID列表，若提供则直接使用，不再从Redis读取

    返回：
        故障模式推导图的结构化数据（JSON 字符串），包含节点和边的关系
    """
    # 优先使用传入的 defect_ids，为空时才从 Redis 读取
    if defect_ids:
        query_defect_ids = defect_ids
        logger.info(f"cg_graphshow: Using provided defect_ids directly: {query_defect_ids}")
    else:
        redis_key = f"{thread_id}_cached_defectIds"
        if memoryRedis.has_key(redis_key):
            value = memoryRedis.get_cache(redis_key)
            query_defect_ids = value
            logger.info(f"Executing tool key: {redis_key}, value: {value}")
        else:
            logger.warning(f"cg_graphshow: No defect_ids provided and Redis key not found: {redis_key}")
            return "未找到有效的故障ID，请传入defect_ids或确保诊断上下文中已缓存"

    logging.info(f"cg_graphshow final query ids: {query_defect_ids}")

    java_url = os.getenv("SERVER_URL") + "/device/graph/show"

    response = requests.post(
        url=java_url,
        json=query_defect_ids,
        headers={"Content-Type": "application/json"}
    )
    logging.info(f"response.status_code: {response.status_code}")

    if response.status_code == 200:
        logging.info(f"cg_graphshow response: {response.text}")
        return response.text
    else:
        return "发送失败"

@mcp.tool()
def cg_deviceRag(thread_id: str = "",
                 defect_ids: Optional[List] = None) -> str:
    """
    查询设备故障知识库（RAG），获取与当前诊断相关的辅助知识。

    基于当前诊断上下文，从知识库中检索相关的历史故障案例、
    处理经验、运维建议等信息，用于补充诊断结论和生成处理建议。

    适用场景：
    - 已完成故障模式分析，需要补充历史案例和处理经验
    - 生成运维建议时需要知识库支撑
    - 用户询问"以前有没有发生过类似故障""以前怎么处理的"

    参数：
        thread_id: DeerFlow 执行线程 ID，用于关联当前诊断上下文
        defect_ids: 故障ID列表，若提供则直接使用，不再从Redis读取，格式为

    返回：
        知识库检索结果（JSON 字符串），包含相关故障案例和处理建议。
        未检索到相关内容时返回空结果，此时应跳过知识增强步骤，
        不得自行编造知识。

    注意：
        RAG 结果仅作为辅助参考，不得覆盖实际的诊断数据和测点分析结论。
    """
    # 优先使用传入的 defect_ids，为空时才从 Redis 读取
    if defect_ids:
        query_defect_ids = defect_ids
        logger.info(f"cg_deviceRag: Using provided defect_ids directly: {query_defect_ids}")
    else:
        redis_key = f"{thread_id}_cached_defectIds"
        if memoryRedis.has_key(redis_key):
            value = memoryRedis.get_cache(redis_key)
            query_defect_ids = value
            logger.info(f"Executing tool key: {redis_key}, value: {value}")
        else:
            logger.warning(f"cg_deviceRag: No defect_ids provided and Redis key not found: {redis_key}")
            return "未找到有效的故障ID，请传入defect_ids或确保诊断上下文中已缓存"

    logging.info(f"cg_deviceRag final query ids: {query_defect_ids}")

    java_url = os.getenv("SERVER_URL") + "/device/rag/v2"

    response = requests.post(
        url=java_url,
        json=query_defect_ids,
        headers={"Content-Type": "application/json"}
    )
    logging.info(f"response.status_code: {response.status_code}")

    if response.status_code == 200:
        logging.info(f"cg_deviceRag response: {response.text}")
        return response.text
    else:
        return "发送失败"
        

# 查询rag信息 -- deviceRag_V1
@mcp.tool()
def deviceRag_V1(ragInfo: str, thread_id: str = "") -> str:
    """

    :param ragInfo: 待检索关键信息
    :param thread_id: 线程id    
    Returns:
        字符串
    """
    logging.info(f"deviceRag_v1: {ragInfo}")
    java_url = os.getenv("SERVER_URL")+"/device/rag"
    params = {}
    params["tagName"] = ragInfo
    response = requests.post(url=java_url, params=params, headers={"Content-Type": "application/json"})
    logging.info(f"response.status_code: {response.status_code}")
    if response.status_code == 200:
        logging.info(f"deviceRag_V1 response: {response.text}")
        return response.text
    else:
        return "发送失败"

# 显示测点实际值-- cg_tagsRealtimeValues
@mcp.tool()
def cg_tagsRealtimeValues( thread_id: str = "") -> str:
    """
    获取诊断单关联测点的实时统计数据。

    根据当前诊断上下文，查询诊断单关联测点在生成时间前后半小时内的
    实时运行数据，返回每个测点的时序数值数组，用于分析参数变化趋势
    和判断是否超限。

    返回数据包含：
    - 查询时间范围
    - 每个测点的名称、单位、严重度等级、测点值数组（按时序排列）

    适用场景：
    - 获取诊断单关联测点的实时运行数据
    - 分析测点参数在诊断时间前后的变化趋势
    - 判断测点数据是否与故障模式吻合

    参数：
        thread_id: DeerFlow 执行线程 ID，用于关联当前诊断上下文

    返回：
        测点统计数据（字符串），格式示例：
        测点名称：xxx, 单位：Mpa, 严重度等级：征兆,
        测点值：[15.57, 15.57, 15.56, ...]

    注意：
        - 测点值数组为时序数据，按等间隔采样
        - 单位为空时表示该测点为开关量（0/1 表示开/关状态）
        - 严重度等级表示该测点在故障诊断中的重要程度
    """
    cached_defectIds: list
    redis_key = f"{thread_id}_cached_defectIds"
    if memoryRedis.has_key(redis_key):
        value = memoryRedis.get_cache(redis_key)
        cached_defectIds = value
        logger.info(f"Executing tool key: {redis_key}, value: {value}")
    logging.info(f"cg_tagsRealtimeValues: {cached_defectIds}")
    java_url = os.getenv("SERVER_URL")+"/device/tagsRealTime"
    response = requests.post(url=java_url, json=cached_defectIds, headers={"Content-Type": "application/json"})
    logging.info(f"response.status_code: {response.status_code}")
    if response.status_code == 200:
        logging.info(f"cg_tagsRealtimeValues response: {response.text}")
        return response.text
    else:
        return "发送失败"

# 获取测点信息 -- cg_tagsInfoList
@mcp.tool()
def cg_tagsInfoList( thread_id: str = "") -> str:
    """
    获取诊断单关联测点的元数据信息。

    根据当前诊断上下文，返回诊断单关联测点的基础信息，
    包括测点编码、所属子系统 ID、以及对应的数据查询时间范围。
    用于在获取测点实时数据前，确认测点身份和查询区间。

    返回数据包含每个测点的：
    - subsystemId：所属子系统 ID
    - tagCode：测点编码（唯一标识）
    - beginTime / endTime：数据查询时间范围

    适用场景：
    - 获取诊断单关联测点的编码和时间范围，为后续查询实时数据做准备
    - 确认诊断单关联了哪些测点

    参数：
        thread_id: DeerFlow 执行线程 ID，用于关联当前诊断上下文

    返回：
        测点元数据列表（JSON 字符串），格式示例：
        {
          "cached_TagsTrendPara": [
            {
              "subsystemId": 739210,
              "tagCode": "DC01R0101PAC01GP292QN161XB011",
              "beginTime": "2025-12-15T00:37:00Z",
              "endTime": "2025-12-15T01:07:00Z"
            }
          ]
        }

    注意：
        - tagCode 是测点的唯一标识，用于后续调用 cg_tagsRealtimeValues 等接口
        - beginTime/endTime 为 UTC 时间（+00:00），展示给用户时需转换为北京时间（+08:00）
    """
    cached_defectIds: list
    redis_key = f"{thread_id}_cached_defectIds"
    if memoryRedis.has_key(redis_key):
        value = memoryRedis.get_cache(redis_key)
        cached_defectIds = value
        logger.info(f"Executing tool key: {redis_key}, value: {value}")
    logging.info(f"cg_tagsInfoList: {cached_defectIds}")
    java_url = os.getenv("SERVER_URL")+"/device/tagsInfoList"
    response = requests.post(url=java_url, json=cached_defectIds, headers={"Content-Type": "application/json"})
    logging.info(f"response.status_code: {response.status_code}")
    if response.status_code == 200:
        logging.info(f"cg_tagsInfoList response: {response.text}")
        return response.text
    else:
        return "发送失败"
# 获取所有机组 -- get_unit_list
@mcp.tool()
def get_unit_list() -> str:
    """
    获取所有机组
    Returns:
        字典信息
    """

    java_url = os.getenv("SERVER_URL")+"/unit/getUnitList"
    response = requests.post(url=java_url)
    logging.info(f"response.status_code: {response.status_code}")
    if response.status_code == 200:
        logging.info(f"cg_tagsInfoList response: {response.text}")
        return response.text
    else:
        return "发送失败"
# 获取指定机组下所有指标 -- query_indicators
@mcp.tool()
def query_indicators(unit_id: int) -> str:
    """
    获取指定机组下所有指标
    parma: unit_id: 机组ID，
    Returns:
        字典信息
    """
    payload = {}
    payload["unitId"] = unit_id
    java_url = os.getenv("SERVER_URL")+"/unit/getIndicators"
    response = requests.post(url=java_url,params=payload)
    logging.info(f"response.status_code: {response.status_code}")
    if response.status_code == 200:
        logging.info(f"cg_tagsInfoList response: {response.text}")
        return response.text
    else:
        return "发送失败"

# @mcp.tool()
def cg_tagTrend(
        orginal: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        thread_id: str = "") -> str:
    """测点的趋势信息
    Args:
        orginal: 原文
        start_time: 开始时间（可选），如 "2024-01-01T00:00:00+08:00"
        end_time: 结束时间（可选），如 "2024-01-07T23:59:59+08:00"
        thread_id: 线程id
    Returns:
        字符串
    """
    logging.info(f"cg_tagTrend: {orginal}, {start_time}, {end_time}")
    
    # 如果匹配到测点，就用测点的
    code = None
    try:
        match_result = _match_for_best_impl(orginal)
        logging.info(f"match_result: {match_result}")
        logging.info(f"match_result type: {type(match_result)}")
        if isinstance(match_result, str):
            match_result = json.loads(match_result)
        code = match_result["data"][0]["code"]
        logging.info(f"name: {code}")
    except Exception as e:
        logger.info(f"error: {str(e)}")
    
    if code is not None:
        payload = {}
        payload["tagName"] = code
        # payload["srcTagName"] = code
        payload["type"] = 'RealTimeData'

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

        url = f"{server_url}/tag/tagValues"
        try:
            logger.info(f"cg_tagTrend POST 请求发送至: {url}, 参数: {payload}")
            resp = requests.post(url, params=payload)
            # resp.raise_for_status()
            # logging.info(f"cg_tagTrend response: {response.text}")
            # return response.text
            if resp.status_code == 200:
                logging.info(f"cg_tagTrend response: {resp.text}")
                return resp.text
            else:
                return "发送失败"
        except requests.exceptions.HTTPError as e:
            logger.info(f"错误：后端接口请求失败，状态码：{e.response.status_code}")
            return f"错误：后端接口请求失败，状态码：{e.response.status_code}"
        except Exception as e:
            logger.info(f"错误：请求异常: {str(e)}")
            return f"错误：请求异常: {str(e)}"
    else:
        # 如果没有匹配到测点，就认为是追问或多意图
        logger.info("name is null")
        java_url = os.getenv("SERVER_URL")+"/device/tagsTrend"
        response = requests.post(url=java_url, json=match_result, headers={"Content-Type": "application/json"})
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

# @mcp.tool()
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
        unit_name: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        closed: Optional[bool] = None,
) -> str:
    """
    查询机组下诊断单列表。
    根据机组名模糊匹配机组，返回诊断单信息（含 incidentId 等）。

    Args:
        unit_name: 机组名称（可选），如 "京燃"
        start_time: 开始时间（格式:YYYY-MM-DDT00:00:00Z）
        end_time: 结束时间（格式:YYYY-MM-DDT00:00:00Z）
        closed: 是否已关闭（可选）
    """
    payload = {}
    if unit_name: payload["unitName"] = unit_name
    if start_time: payload["startTime"] = start_time
    if end_time: payload["endTime"] = end_time
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
    获取诊断单的故障模式推导图（知识图谱）。

    根据诊断单 ID 列表，返回每个诊断单对应的故障模式层级关系，
    展示从故障模式到触发特征再到关联测点的推导链路，
    用于分析故障机理、判断异常根因。

    返回结构为树形层级：
    - [故障模式] 某某异常
        - [特征] 某某参数异常
            - [测点] 某某测点名称
        - [特征] 某某状态变化
            - [测点] 某某测点名称

    适用场景：
    - 分析诊断单对应的故障模式及其触发条件
    - 了解故障模式由哪些特征和测点支撑
    - 判断测点数据是否与故障模式吻合

    参数：
        incident_ids: 诊断单 ID 列表，如 [123, 456]，支持批量查询

    返回：
        故障模式推导图（字符串），树形层级格式，包含故障模式、
        特征、测点三层关系。

    注意：
        - 每个诊断单独立返回，不要将不同诊断单的推导图合并
        - 返回的测点名称可用于后续调用 cg_tagsRealtimeValues 获取实时数据
    """
    url = f"{server_url}/device/graph/showV2"
    results = []
    _realtime_pattern = re.compile(
        r'^[ \t]*(?://\s*realTimeValue.*|Double\s+tag_\S+\s*=\s*realTimeValue\([^)]*\);)[ \t]*\n?',
        re.MULTILINE
    )
    # for iid in incident_ids:
    #     payload = [{"incidentId": iid}]
    #     try:
    #         logger.info(f"POST 请求发送至: {url}, 参数: {payload}")
    #         resp = requests.post(url, json=payload, headers={"Content-Type": "application/json"})
    #         resp.raise_for_status()
    #         cleaned_text = _realtime_pattern.sub('', resp.text)
    #         results.append(f"# 诊断单ID: {iid}\n{cleaned_text}")
    #     except requests.exceptions.HTTPError as e:
    #         results.append(f"# 诊断单ID: {iid}\n错误：后端接口请求失败，状态码：{e.response.status_code}")
    #     except requests.exceptions.RequestException as e:
    #         results.append(f"# 诊断单ID: {iid}\n错误：请求异常: {str(e)}")
    # if not results:
    #     return "未获取到任何诊断单的故障模式推导图，请检查输入的诊断单ID列表。"
    # return "\n\n---\n\n".join(results)
    return _batch_post(
        url,
        incident_ids,
        formatter=lambda s: _realtime_pattern.sub("", s)
    )

@mcp.tool()
def default_graph_detail(
        default_name: Optional[str] = None,
        tag_list: Optional[List] = None,
        device_name: Optional[str] = None,
) -> str:
    """
    查询故障模式详情（故障树结构）。

    支持三种查询方式，按优先级从高到低：
    1. 输入 default_name（故障名称）：模糊匹配最相似的故障模式，返回其完整详情
    2. 输入 tag_list（测点 ID 列表）：匹配同时包含这些测点的故障模式，返回其完整详情
    3. 输入 device_name（设备名称）：返回该设备下所有故障模式名称列表

    三者均输入时，优先按故障模式名称匹配。三者均不输入时，返回未匹配到的固定提示信息。

    返回的故障模式详情包含：
    - 节点列表：故障模式节点、特征节点、测点节点及其属性
    - 关系列表：节点间的因果关系、触发条件（偏大/偏小/波动等）和权重
    - 结构摘要：节点数、关系边数
    - 触发逻辑说明：当满足连线与节点关系时触发故障模式报警

    适用场景：
    - 查询某个故障模式的具体触发条件和计算逻辑
    - 查询某设备下有哪些故障模式
    - 根据测点反查对应的故障模式

    参数：
        default_name: 故障名称（纯文本），如"循环水泵A汽蚀"，模糊匹配
        tag_list: 测点 ID 列表（整数），如 [1, 2, 3]，注意是测点 ID，不是故障模式 ID 或设备 ID
        device_name: 设备名称（纯文本），如"机组循环水泵A"

    返回：
        故障模式详情（JSON 字符串），包含节点列表、关系列表、结构摘要和触发逻辑说明。
    """
    payload = {}
    if default_name: payload["defectName"] = default_name
    if tag_list: payload["tagLists"] = tag_list
    if device_name: payload["deviceName"] = device_name

    url = f"{server_url}/tag/getGraphByTagList"
    try:
        logger.info(f"POST 请求发送至: {url}, 参数: {payload}")
        resp = requests.post(url, params=payload, headers={"Content-Type": "application/json"})
        resp.raise_for_status()
        return resp.text
    except requests.exceptions.HTTPError as e:
        return f"错误：后端接口请求失败，状态码：{e.response.status_code}"
    except requests.exceptions.RequestException as e:
        return f"错误：请求异常: {str(e)}"

@mcp.tool()
def unit_tags_realtime(incident_ids: List[int]) -> str:
    """
    获取诊断单关联测点的统计数据（诊断时间前后半小时）。

    根据诊断单 ID 列表，返回每个诊断单关联测点在生成时间前后半小时内的
    统计数据，包括实际值、估计值、严重度和数据分布统计，用于分析参数
    是否异常以及异常程度。

    返回数据包含每个测点的：
    - 所属系统/子系统名称
    - 测点名称和单位
    - 实际值统计：最小值、最大值、平均值
    - 估计值统计：最小值、最大值、平均值
    - 严重度统计：最小值、最大值、平均值
    - 数据分布：偏低次数、正常次数、偏高次数

    适用场景：
    - 分析诊断单关联测点的运行数据是否异常
    - 判断测点数据与故障模式的吻合程度
    - 评估异常的严重程度和持续范围

    参数：
        incident_ids: 诊断单 ID 列数，如 [123, 456]，支持批量查询

    返回：
        测点统计数据（JSON 字符串），包含 cols（列名）和 data（数据行）。
        列名含义：
        - systemName / subsystemName：所属系统/子系统
        - tagName / unit：测点名称/单位
        - realTimeDateMin/Max/Avg：实际值最小/最大/平均
        - estimateMin/Max/Avg：估计值最小/最大/平均
        - severityMin/Max/Avg：严重度最小/最大/平均
        - XXLowNum / normalNum / XXHignum：偏低/正常/偏高次数
    """
    url = f"{server_url}/device/tagsRealTimeV2"
    if incident_ids is not None: payload = incident_ids
    try:
        logger.info(f"POST 请求发送至: {url}, 参数: {payload}")
        resp = requests.post(url, json=payload, headers={"Content-Type": "application/json"})
        resp.raise_for_status()
        return resp.text
    except requests.exceptions.HTTPError as e:
        return f"错误：后端接口请求失败，状态码：{e.response.status_code}"
    except requests.exceptions.RequestException as e:
        return f"错误：请求异常: {str(e)}"
    # return _batch_post(url, incident_ids)

@mcp.tool()
def unit_device_rag(incident_ids: List[int]) -> str:
    """
    根据诊断单 ID 检索知识库，获取相关历史知识和处理建议。

    根据诊断单关联的故障模式，从知识库中检索对应的运行规程、
    历史故障案例和处理经验，用于补充诊断结论和生成运维建议。

    返回按故障模式分组的知识条目，每条包含来源文档和具体内容。

    返回示例：
    {
      "循环水泵B液控蝶阀油压异常": {
        "full-集控运行规程.docx": [
          "循环水泵出口液控蝶阀系统压力维持在11.5-16.5MPa..."
        ]
      }
    }

    适用场景：
    - 已完成故障模式分析，需要补充历史案例和处理经验
    - 生成运维建议时需要运行规程支撑
    - 用户询问"以前有没有发生过类似故障""以前怎么处理的"

    参数：
        incident_ids: 诊断单 ID 列表，如 [123, 456]，支持批量查询

    返回：
        知识库检索结果（JSON 字符串），按故障模式名称分组，
        包含来源文档名和知识内容。未检索到时返回空对象。

    注意：
        - RAG 结果仅作为辅助参考，不得覆盖实际的诊断数据和测点分析结论
        - 未检索到相关内容时跳过知识增强步骤，不得自行编造知识
    """
    url = f"{server_url}/device/rag/v3"
    # results = []
    # for iid in incident_ids:
    #     payload = [{"incidentId": iid}]
    #     try:
    #         logger.info(f"POST 请求发送至: {url}, 参数: {payload}")
    #         resp = requests.post(url, json=payload, headers={"Content-Type": "application/json"})
    #         resp.raise_for_status()
    #         results.append(f"# 诊断单ID: {iid}\n{resp.text}")
    #     except requests.exceptions.HTTPError as e:
    #         results.append(f"# 诊断单ID: {iid}\n错误：后端接口请求失败，状态码：{e.response.status_code}")
    #     except requests.exceptions.RequestException as e:
    #         results.append(f"# 诊断单ID: {iid}\n错误：请求异常: {str(e)}")
    # if not results:
    #     return "未获取到任何诊断单的RAG知识，请检查输入的诊断单ID列表。"
    # return "\n\n---\n\n".join(results)
    return _batch_post(url, incident_ids)

@mcp.tool()
def unit_mount_path(
        unit_id: int,
        incident_ids: List[int],
) -> str:
    """
    获取机组下诊断单关联节点的层级路径树。
    根据机组ID和诊断单ID列表，查询各诊断单对应节点的完整层级路径（从机组到节点），
    Args:
        unit_id: 机组ID（必填）
        incident_ids: 诊断单ID列表（必填），如 [123, 456]
    """
    url = f"{server_url}/unit/getPathUnderUnit"
    payload = {"unitId": unit_id, "incidentIds": incident_ids}
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
def get_alarm_list(
        unit_id: Optional[int] = None,
        tag_code: Optional[list[str]] = None,
        tag_source_name: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        asset_id: Optional[int] = None,
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
        tag_code: 测点编码列表（可选）
        tag_source_name: 测点源标签点名（可选）
        start_time: 开始时间（可选），查询 firsttouchtime >= 该时间的告警
        end_time: 结束时间（可选），查询 lasttouchtime <= 该时间的告警
        asset_id: 设备id（可选），可根据设备id筛选报警单
        data_type: 数据类型（可选），如 "告警"、"缺陷" 等
        current_status_name: 当前状态名称（可选），如 "新报警单"、"已关闭"等
        tag_id: 测点ID（可选），精确查询某个测点的告警
        monitor_point_id: 监测点ID（可选）
        closed: 是否已关闭（可选），true表示查询已关闭的告警，false表示未关闭的，all表示为全部，默认为false
    """
    payload = {}
    if unit_id is not None: payload["unitId"] = unit_id
    if tag_code: payload["tagNames"] = tag_code
    if tag_source_name: payload["tagSourceName"] = tag_source_name
    if start_time: payload["startTime"] = start_time
    if end_time: payload["endTime"] = end_time
    if asset_id is not None: payload["assetId"] = asset_id
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
def unit_alarm_list_statistics(
        unit_id: Optional[int] = None,
        tag_code: Optional[str] = None,
        tag_source_name: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        asset_id: Optional[int] = None,
        data_type: Optional[str] = None,
        current_status_name: Optional[str] = None,
        tag_id: Optional[int] = None,
        monitor_point_id: Optional[int] = None,
        closed: Optional[bool] = None,
        group_by_asset_name: Optional[bool] = None,
) -> str:
    """
    查询报警单统计内容(与数据趋势无关)。支持多维度筛选告警信息。
    查询报警列表，返回报警类型解释及统计不同报警类型下的测点信息
    Args:
        unit_id: 机组ID（可选），查询特定机组下的所有告警
        tag_code: 测点编码（可选）
        tag_source_name: 测点源标签点名（可选）
        start_time: 开始时间（可选），查询 firsttouchtime >= 该时间的告警
        end_time: 结束时间（可选），查询 lasttouchtime <= 该时间的告警
        asset_id: 设备id（可选）
        data_type: 数据类型（可选），如 "告警"、"缺陷" 等
        current_status_name: 当前状态名称（可选），如 "新报警单"、"已关闭"等
        tag_id: 测点ID（可选），精确查询某个测点的告警
        monitor_point_id: 监测点ID（可选）
        closed: 是否已关闭（可选），true表示查询已关闭的告警，false表示未关闭的，all表示为全部，默认为false
        group_by_asset_name: 是否按照设备名称分组统计，默认不传该参数
    """
    payload = {}
    if unit_id is not None: payload["unitId"] = unit_id
    if tag_code: payload["tagName"] = tag_code
    if tag_source_name: payload["tagSourceName"] = tag_source_name
    if start_time: payload["startTime"] = start_time
    if end_time: payload["endTime"] = end_time
    if asset_id is not None: payload["assetId"] = asset_id
    if data_type: payload["dataType"] = data_type
    if current_status_name: payload["currentStatusName"] = current_status_name
    if tag_id is not None: payload["tagId"] = tag_id
    if monitor_point_id is not None: payload["monitorPointId"] = monitor_point_id
    if closed is not None: payload["closed"] = closed
    if group_by_asset_name: payload["groupByAssetName"] = group_by_asset_name

    url = f"{server_url}/unit/getAlarmListStatistics"
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
    查询系统评估单列表。支持多维度筛选系统级评估单信息。

    Args:
        unit_id: 机组ID（可选），查询特定机组下的系统评估单
        system_id: 系统ID（可选），查询特定系统的评估单
        start_time: 开始时间（可选），与 end_time 配合使用
        end_time: 结束时间（可选），与 start_time 配合使用
        current_status: 当前状态（可选），如 "待处理"、"处理中"、"已关闭"
        closed: 是否已关闭（可选），true表示查询已关闭的评估单，false表示未关闭的，all表示为全部，默认为false
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
    查询子系统评估单列表。支持多维度筛选子系统级评估单信息。

    Args:
        unit_id: 机组ID（可选），查询特定机组下的子系统评估单
        sub_system_id: 子系统ID（可选），查询特定子系统的评估单
        start_time: 开始时间（可选），与 end_time 配合使用
        end_time: 结束时间（可选），与 start_time 配合使用
        current_status: 当前状态（可选），如 "待处理"、"处理中"、"已关闭"
        closed: 是否已关闭（可选），true表示查询已关闭的评估单，false表示未关闭的，all表示为全部，默认为false
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

@mcp.tool()
def get_instance_of_unit(
        unit_id: int,
        instance_type: str,
        keyword: Optional[str] = None
) -> str:
    """
    查询机组下的设备层级实例（系统/子系统/设备/部件/测点）。

    根据机组 ID 查询其下属的设备层级结构，支持按关键词筛选和按实例类型过滤。
    用于获取设备层级路径、确认设备名称、以及为后续查询测点数据做准备。

    返回每个实例的编码、名称、类别、描述、方位码等属性信息。

    适用场景：
    - 查询机组下有哪些设备/系统/部件
    - 根据关键词模糊搜索设备名称
    - 确认设备的编码和层级关系，为后续测点查询做准备

    Args:
        unit_id: 机组 ID，查询特定机组下的设备
        keyword: 关键词（可选），查询与关键词相关的设备，如"循环水泵""给水泵"
        instance_type: 实例类型，只能是以下之一：
            - "系统"：如机组循环水系统
            - "子系统"：如机组循环水输送系统
            - "设备"：如机组循环水泵A
            - "部件"：如循环水泵A驱动端轴承
            - "测点"：如循环水泵A电机电流
            不传则返回所有类型的实例

    Returns:
        实例属性列表（JSON 字符串），每项包含：
        - 编码：实例唯一标识
        - 名称：实例全名（含机组前缀）
        - 类别：实例类型（系统/子系统/设备/部件/测点）
        - 描述：设备类型说明（如"离心风机""电动机"）
        - 短码：简短编码
    """
    payload = {}
    payload["unitId"] = unit_id
    if keyword: payload["keyword"] = keyword
    payload["instanceType"] = instance_type

    url = f"{server_url}/unit/getInstanceOfUnit"
    try:
        logger.info(f"POST 请求发送至: {url}, 参数: {payload}")
        resp = requests.post(url, params=payload)
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
        orginal: str,
        tag_id: Optional[int] = None,
        tag_code: Optional[str] = None,
        src_tag_name: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        type: str = "RealTimeData",
        interval: Optional[int] = 3600,
        thread_id: str = ""
) -> None | str | dict[Any, Any]:
    """
    测点历史数据查询工具，查趋势。
    通过测点ID、编码或源标签点名精确查找指定时间段内的测点数据type,包括:
    - 实际值(RealTimeData)
    - 估计值(Estimate)
    - 严重度(TagSeverity)
    - XX(XX)

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
        thread_id: 线程id
    Returns:
        测点历史数据列表,包含时间戳、实际值、估计值、严重度等信息
    """
    logger.info(f"get_tag_values: {orginal}, {tag_id}, {tag_code}, {src_tag_name}, {start_time}, {end_time}, {type}, {interval}, {thread_id}")
    # 从原文提取下，看有没有测点如果匹配到测点，就用测点的
    code = None
    try:
        match_result = _match_for_best_impl(orginal)
        logging.info(f"match_result: {match_result}")
        logging.info(f"match_result type: {type(match_result)}")
        if isinstance(match_result, str):
            match_result = json.loads(match_result)
        code = match_result["data"][0]["code"]
        logging.info(f"name: {code}")
    except Exception as e:
        logger.info(f"error: {str(e)}")
    if code is not None:
        # 从redis获取 cached_TagsTrendPara
        logger.info("redis: 1")
        cached_defectIds: list
        redis_key = f"{thread_id}_cached_defectIds"
        if memoryRedis.has_key(redis_key):
            value = memoryRedis.get_cache(redis_key)
            cached_defectIds = value
            logger.info(f"Executing tool key: {redis_key}, value: {value}")
    else:
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
            trend_result = resp.text
            all_msg = {}
            all_msg["llmMsg"] = f"{tag_code}, {src_tag_name} 趋势查询成功"
            all_msg["result_trend"] = trend_result
            return all_msg
        except requests.exceptions.HTTPError as e:
            return f"错误：后端接口请求失败，状态码：{e.response.status_code}"
        except requests.exceptions.RequestException as e:
            return f"错误：请求异常: {str(e)}"
#============查询测点某时刻的值=============================================================
@mcp.tool()
def get_tag_value_attime(
        tag_id: Optional[int] = None,
        tag_code: Optional[str] = None,
        src_tag_name: Optional[str] = None,
        time: Optional[str] = None,
        type: str = "RealTimeData",
) -> str:
    """
    查询测点某单一时刻的值
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
        time: 开始时间(可选),格式如 "2024-01-01T00:00:00+08:00",不传则默认为6小时前
        type: 查询类型(必填),默认为实际值(RealTimeData),可选值: RealTimeData,Estimate,TagSeverity,all
        thread_id: 线程id
    Returns:
        测点某时刻的值
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

    if time:
        payload["time"] = time
    payload["type"] = type

    # 如果没有传时间,就最新时刻的一个值

    url = f"{server_url}/tag/tagValue"
    try:
        logger.info(f"POST 请求发送至: {url}, 参数: {payload}")
        resp = requests.post(url, params=payload)
        resp.raise_for_status()
        logger.info(f"get_tag_value resp: {resp.text}")
        return resp.text
    except requests.exceptions.HTTPError as e:
        logger.info(f"get_tag_value error 状态码：{e.response.status_code}")
        return f"错误：后端接口请求失败，状态码：{e.response.status_code}"
    except requests.exceptions.RequestException as e:
        logger.info(f"get_tag_value error 请求异常: {str(e)}")
        return f"错误：请求异常: {str(e)}"

@mcp.tool()
def get_tags_of_instance(
        type: str,
        parent_name: str,
        tagType: str
) -> str:
    """
    获取指定实体（系统/子系统/设备）下的测点列表。

    根据实体类型和名称，返回该实体下所有测点的基础信息，
    用于后续查询测点实时数据、趋势分析或故障诊断。

    返回每个测点的：
    - 测点编码（tagCode）和 ID（tagId）
    - 测点名称和原始测点名称
    - 所属系统/子系统/设备的层级信息和 ID
    - 单位（如 MPa、℃、rpm 等）

    适用场景：
    - 查询某设备下有哪些模拟量/开关量测点
    - 获取测点编码和 ID，为后续调用 get_tag_statistic_data 等接口做准备
    - 分析前确认目标实体的测点清单

    Args:
        type: 实体类型，即 parent_name 对应的类型，可选值：
            - "设备"：如"机组循环水泵D"
            - "子系统"：如"机组循环水输送系统"
            - "系统"：如"机组循环水系统"
        parent_name: 实体名称，需与系统中的名称完全匹配
        tagType: 测点类型，必填，可选值：
            - "模拟量"：温度、压力、振动、电流、流量等连续值
            - "开关量"：开/关、分/合闸等离散状态

    Returns:
        测点信息列表（JSON 字符串），每项包含：
        - tagCode / tagId：测点编码和 ID
        - name：测点全名（含机组前缀）
        - unit：单位（模拟量有值，开关量可能为空）
        - systemName / subSystemName / deviceName：所属层级名称
        - srcTagName / srcTagDesc：原始测点编码和描述

    注意：
        - 查询趋势/数值类问题用"模拟量"，查询开闭状态用"开关量"
        - 实体名称必须精确匹配，模糊名称可能返回空结果
        - 返回的 tagId 可用于后续调用 get_tag_statistic_data 获取统计数据
    """
    if not type or not parent_name:
        return "参数不得为空：type 和 parent_name 均为必填参数"
    payload = {}
    if type:payload["type"] = type
    if parent_name:payload["parentName"] = parent_name
    payload["tagType"] = tagType
    url = f"{server_url}/tag/getAllTags"
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
def get_tag_statistic_data(
        tag_id: Optional[int] = None,
        tag_code: Optional[str] = None,
        src_tag_name: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        parent_name: Optional[str] = None
) -> str:
    """
    获取具体某一测点在一段时间内的统计数据，包括实际值、估计值、严重度、XX数量。
    三个标识参数(tag_id/tag_code/src_tag_name)只需填写一个即可,优先级为: tag_code > tag_id > src_tag_name
    Args:
        :param tag_id: 测点ID(可选),精确匹配
        :param tag_code: 测点编码(可选),精确匹配
        :param src_tag_name: 源标签点名(可选),精确匹配
        :param start_time: 开始时间(可选),格式如 "2024-01-01T00:00:00+08:00"
        :param end_time: 结束时间(可选),格式如 "2024-01-07T23:59:59+08:00"
        :param parent_name: 父级示例名称(可选),不提及此参数时默认不传
    Returns:
        实体下所有测点统计信息，若配置了父级示例名称，则返回父级示例名称下的所有测点统计信息
    """
    if not (tag_id or tag_code or src_tag_name): return "至少传入一个测点参数！"
    if not start_time and not end_time:
        from datetime import datetime, timedelta, timezone
        now = datetime.now(timezone.utc)
        six_hours_ago = now - timedelta(hours=6)
        start_time = six_hours_ago.strftime("%Y-%m-%dT%H:%M:%S+00:00")
        end_time = now.strftime("%Y-%m-%dT%H:%M:%S+00:00")
    payload = {}
    if tag_id:payload["tagId"] = tag_id
    if tag_code:payload["tagName"] = tag_code
    if src_tag_name:payload["srcTagName"] = src_tag_name
    if start_time:payload["startTime"] = start_time
    if end_time:payload["endTime"] = end_time
    if parent_name:payload["parentName"] = parent_name

    url = f"{server_url}/tag/tagStatisticData"
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
def get_environmental_indicator_infos(
        keyword: Optional[str] = None,
) -> str:
    """
    根据某一关键词查询相关的指标或者查询全部的指标。
    Args:
        keyword: 关键词(可选)，不配置默认查全部
    Returns:
        返回与关键词相关的指标与对应的限值
"""
    payload = {}
    if keyword:payload["fuzzyName"] = keyword
    url = f"{server_url}/tag/selectEnvironmentalExamplesByFuzzyMatching"
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
def get_deep_peak_statistic(
        unit_id,
        load_rate,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None
) -> str:
    """
    深度调峰运行分析。用于分析机组在指定时间范围内的深度调峰运行情况
    Args:
        unit_id: 机组id
        load_rate: 负荷率阈值
        start_time: 开始时间(可选),格式如 "2024-01-01T00:00:00+08:00",不传则默认为6小时前
        end_time: 结束时间(可选),格式如 "2024-01-07T23:59:59+08:00",不传则默认为当前时间
    Returns:
        返回统计数据：时间区间、最小值与对应时间
"""
    payload = {}
    payload["unitId"] = unit_id
    payload["loadRate"] = load_rate
    if start_time:payload["startTime"] = start_time
    if end_time:payload["endTime"] = end_time
    url = f"{server_url}/tag/getDeepPeakStatistic"
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
def get_last_time_by_switch_name(
        tagCode: str,
        value: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None
) -> str:
    """
    查询开关量的跳变时间记录。

    支持两种查询模式（二选一，不可同时使用）：

    模式1：查询开关量等于指定值的最新时间
    - 传入 tagCode + value，不传 start_time 和 end_time
    - 返回该开关量最后一次等于 value 的时间

    模式2：查询时间窗口内所有跳变记录
    - 传入 tagCode + start_time + end_time，不传 value
    - 返回该时间段内每次状态变化的时间戳和方向（0→1 或 1→0）

    返回格式示例：
    模式1："时间：2026-06-14T01:15:00Z, 值: 1.000000"
    模式2："0to1":"2026-04-22T06:20:00Z"  "1to0":"2026-04-22T06:21:00Z"

    适用场景：
    - 查询某开关量当前处于什么状态（开/关）
    - 查询某开关量最近一次动作是什么时候
    - 分析开关量在一段时间内的动作频率和规律
    - 查询集电线路分闸断开状态、跳闸时间等

    Args:
        tagCode: 测点编码
        value: 开关量指定值（"0"=关/分闸，"1"=开/合闸），仅模式1使用
        start_time: 查询窗口起始时间（仅模式2使用），
            格式为北京时间，如 "2026-04-10T15:54:58+08:00"
        end_time: 查询窗口结束时间（仅模式2使用），
            格式为北京时间，如 "2026-04-10T15:54:58+08:00"

    Returns:
        查询结果（JSON 字符串）：
        - 模式1：返回"时间：xxx, 值: x"
        - 模式2：返回多条跳变记录，格式"0to1":"时间" 或 "1to0":"时间"
            - 0to1：从关到开（分闸→合闸）
            - 1to0：从开到关（合闸→分闸）

    注意：
        - 两种模式不可同时使用，传 value 时不传时间，传时间时不传 value
        - 时间格式统一为北京时间（+08:00），返回结果为 UTC 时间（+00:00），
          展示给用户时需转换为北京时间
        - tagCode 通过 get_tags_of_instance 获取，tagType 选"开关量"
    """
    payload = {}
    payload["tagName"] = tagCode
    if value: payload["value"] = value
    if start_time: payload["startTime"] = start_time
    if end_time: payload["endTime"] = end_time
    url = f"{server_url}/tag/getLastTimeBySwitchName"
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
def get_model_info(
        tagId: int
) -> str:
    """
    根据测点ID获取模型信息。
    Args:
        tagId: 测点ID
    Returns:
        模型信息
    """
    payload = {}
    payload["tagId"] = tagId
    url = f"{server_url}/tag/getTagsOfModel"
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
def match_for_best(
        match_string: str,
        match_type: Optional[str] = None
) -> str:
    """
    实例模糊匹配工具。
    根据的“用户输入的完整问题”，从所有实例中模糊匹配出相似度最高的实例。
    匹配逻辑基于混合杰卡德相似度（字符级 + 词级 + 基础杰卡德），对实例名称进行相似度计算。
    使用场景：当用户输入的语句不能精确辨识实体时，可先调用此工具将用户发送的整个语句传入进行模糊匹配，
    获取最可能的实例列表后再进行后续操作。
    Args:
        match_string: 用于模糊匹配的字符串，如设备名称、测点名称等
        match_type: 实例类型，如设备、系统、机组、测点等，默认不传该参数，除非用户指定
    Returns:
        相似度最高的实例信息列表，相似度值最高的有多个，就返回多个，每个实例包含 id、name、code、type、similarity 字段
    """
    return _match_for_best_impl(match_string, match_type)

def _match_for_best_impl(match_string: str, match_type: Optional[str] = None) -> str:
    """底层公共方法：实例模糊匹配，可供本地代码直接调用"""
    url = f"{server_url}/common/matchForBest"
    try:
        logger.info(f"POST 请求发送至: {url}, 参数: matchString={match_string}")
        resp = requests.post(url, params={"matchString": match_string, "matchType": match_type})
        resp.raise_for_status()
        logger.info(f"result: {resp.text}")
        return resp.text
    except requests.exceptions.HTTPError as e:
        logger.info(f"错误：后端接口请求失败，状态码：{e.response.status_code}")
        return f"错误：后端接口请求失败，状态码：{e.response.status_code}"
    except requests.exceptions.RequestException as e:
        logger.info(f"错误：请求异常: {str(e)}")
        return f"错误：请求异常: {str(e)}"

def _batch_post(
        url: str,
        incident_ids: List[int],
        formatter=None,
        max_workers=10,
):
    results = {}
    session = requests.Session()
    def worker(iid):
        payload = [{"incidentId": iid}]
        try:
            logger.info(f"POST 请求发送至: {url}, 参数: {payload}")
            resp = session.post(
                url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=30
            )
            resp.raise_for_status()
            text = resp.text
            if formatter:
                text = formatter(text)
            return iid, f"# 诊断单ID: {iid}\n{text}"
        except requests.exceptions.HTTPError as e:
            return iid, f"# 诊断单ID: {iid}\n错误：后端接口请求失败，状态码：{e.response.status_code}"
        except requests.exceptions.RequestException as e:
            return iid, f"# 诊断单ID: {iid}\n错误：请求异常: {str(e)}"

    with ThreadPoolExecutor(max_workers=min(max_workers, len(incident_ids))) as pool:
        futures = [pool.submit(worker, iid) for iid in incident_ids]
        for future in as_completed(futures):
            iid, result = future.result()
            results[iid] = result

    return "\n---\n".join(results[i] for i in incident_ids if i in results)

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
    uvicorn.run("mcp_server:app", host="0.0.0.0", port=int(os.getenv("PORT")), reload=True)
