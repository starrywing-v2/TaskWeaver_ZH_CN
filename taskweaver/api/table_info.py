import json
import os
import requests
import re
import pandas as pd
from pydantic import BaseModel
from typing import Dict, List


def close_query(url, token):
    requests.post(url=url, data={"token": token})


def is_number(s):
    return bool(re.match(r'^-?\d+(\.\d+)?%?$', s))


def drop_nan_str(row):
    for value in row:
        if isinstance(value, str) and is_number(value):
            return True
        if pd.isna(value):
            return False
    return True


class ResSmartBIApi(object):

    def __init__(self, url: str, data: dict) -> None:
        self.url = url
        self.data = data
        self.res = self.call_api()

    def call_api(self) -> Dict:
        res = requests.post(url=self.url, data=self.data)
        if res.status_code == 200:
            text = json.loads(res.text)
            return text

        else:
            raise f"请求{self.url}失败，res_code={res.status_code}"

    def get_token(self) -> str:
        return self.res.get("token", "")

    def get_html(self) -> str:
        return self.res.get("result", dict()).get("html", "")

    def get_query_condition(self) -> str:
        return self.res.get("result", dict()).get("resultTips", "")

    def get_small_table(self) -> str:
        return self.res.get("result", dict()).get("subTable", "")

    def get_nl2sql(self) -> Dict:
        nl2sql = self.res.get("result", dict()).get("nl2sql", "{}")
        nl2sql = json.loads(nl2sql)
        return nl2sql


class SmallTableData(BaseModel):
    dimension: List[Dict] = []
    measure: List[Dict] = []


class SmallTable:

    def __init__(self, query: str, token: str, model_id: str):
        self.query = query
        self.token = token
        self.model_id = model_id
        self.query_url = "http://10.10.35.110:9070/aiweb/integration/api/v1/query"
        self.close_url = "http://10.10.35.110:9070/aiweb/integration/api/v1/close_query"
        self.res = self.get_res()

    def get_res(self) -> ResSmartBIApi:
        data = {
            "token": self.token,
            "txt": self.query,
            "returnJSON": 'true',
            "themeId": self.model_id
        }
        res = ResSmartBIApi(url=self.query_url, data=data)
        return res

    def build_small_table(self) -> SmallTableData:
        sub_table = self.res.get_small_table()
        try:
            sub_table = json.loads(sub_table)
            sub_table = SmallTableData(**sub_table)
        except json.decoder.JSONDecodeError as e:
            return SmallTableData()
        return sub_table

    @staticmethod
    def build_dimension(small_table: SmallTableData) -> str:
        dim2level = {}
        dimension = ""
        for item in small_table.dimension:
            field_name = item.get("name", "")
            member = ','.join(item.get("member", []))
            member = f"{member}, ..." if member else member
            if item.get("is_level", ""):
                object_name = item.get("objectName", "")
            else:
                object_name = field_name
            template = '\t\t- {} {}\n'
            template = template.format(field_name, f',该层级成员: {member}' if member else '')
            dim2level.setdefault(object_name, []).append(template)
        for dim, level in dim2level.items():
            template = '\t- {},该维度包含以下层级\n{}'
            dimension += template.format(dim, ''.join(level))
        return dimension

    def get_small_table(self) -> str:
        small_table = self.build_small_table()
        dimension = self.build_dimension(small_table)
        measure = [f"\t-{l['name']}" for l in small_table.measure]
        measure = "\n".join(measure)
        small_table = f"cube维度如下：\n{dimension}\n指标如下：\n{measure}"
        close_query(self.close_url, self.token)
        return small_table

    def get_small_table_obj(self) -> SmallTableData:
        close_query(self.close_url, self.token)
        return self.build_small_table()


if __name__ == "__main__":
    import time
    query_token: str = ResSmartBIApi(
        url="http://10.10.35.110:9070/aiweb/api/v1/login",
        data={"userName": "hcy", "password": "hcy"}
    ).get_token()
    time.sleep(0.1)
    st = SmallTable("2023年各月的合同金额", query_token).get_small_table()
    print(st)