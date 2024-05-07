import requests
import json
import pandas as pd


def __get_smartbi_login_cookie(url: str, username: str, password: str):
    """
    获取Smartbi登录后的cookie
    :param url: Smartbi的地址
    :param username: 用户名
    :param password: 密码
    :return: cookie的json格式
    """
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    data = {
        'username': username,
        'password': password
    }
    response = requests.post(url + '/smartbi/vision/index.jsp', headers=headers, data=data)
    result = "JSESSIONID" + "=" + str(response.cookies.get("JSESSIONID"))
    return result


def __request_cellset_by_query_json(url: str, cookies: object, query_json: object):
    """
    从QueryJson获取cellset的返回结果
    :param url: Smartbi的地址
    :param cookies: 登录后的cookie
    :param query_json: 查询的json
    """
    headers = {
        "Content-Type": "application/json; charset=UTF-8",
        'Cookie': cookies
    }
    data = query_json
    data = json.dumps(data)
    response = requests.post(url + '/smartbi/smartbix/api/augmentedQuery/queryCellSetFromSmartCube', headers=headers,
                             data=data)
    return response.json()


def __get_column_dataframe(cell_set: dict):
    """
      获取列区的数据
    """
    df_column_names = []
    for column_row in cell_set.get("columns"):
        column_name = column_row[0].get("name")
        for col_item in column_row[1:]:
            column_name += "|" + col_item.get("name")
        df_column_names.append(column_name)
    col_len = len(df_column_names)
    row_len = len(cell_set.get("rows"))
    if row_len == 0:
        row_len = 1
    df_cells = [[None for _ in range(col_len)] for _ in range(row_len)]
    for data_item in cell_set.get("data"):
        df_cells[data_item.get("row")][data_item.get("column")] = data_item.get("value")
    result = pd.DataFrame(data=df_cells, columns=df_column_names)
    return result


def __get_row_dataframe(cell_set: dict):
    """
    获取行区的数据结构
    """
    if len(cell_set.get("rowFields")) == 0:
        return None
    df_column_names = [c.get("name") for c in cell_set.get("rowFields")]
    df_data = [[i.get("name") for i in row_item] for row_item in cell_set.get("rows")]
    result = pd.DataFrame(data=df_data, columns=df_column_names)
    return result


def parse_dataframe_from_cellset(cell_set: dict):
    """
    从cellset获取分析用的dataframe
    :param cell_set: cellset的返回结果
    """
    # 将数据分为行df和列df然后合并成一个df
    # print(cell_set.get("rowFields"))
    col_df = __get_column_dataframe(cell_set)
    row_df = __get_row_dataframe(cell_set)
    result = pd.concat([row_df, col_df], axis=1)
    return result


def query_cell_set_by_mdx(url: str, smartbi_cookies: str, model_id: str, query_mdx: str):
    data = query_mdx.encode(encoding="utf-8")
    headers = {
        'Content-Type': 'text/plain; charset=UTF-8',
        'Content-Length': str(len(data)),
        'Connection': 'keep-alive',
        'Cookie': smartbi_cookies
    }
    query_url = url + '/smartbi/smartbix/api/augmentedQuery/queryCellSetByMDX/' + model_id
    response = requests.post(query_url,
                             headers=headers, data=data)
    if response.status_code != 200:
        print(f"status code: {response.status_code}")
        print(response.text)
        raise Exception(response.text)
    return response.json()


def query_dataframe_from_mdx(url: str, user_name: str, password: str, model_id: str, query_mdx: str):
    cookie = __get_smartbi_login_cookie(url, user_name, password)
    result_cellset = query_cell_set_by_mdx(url, cookie, model_id, query_mdx)
    result = parse_dataframe_from_cellset(result_cellset)
    return result

