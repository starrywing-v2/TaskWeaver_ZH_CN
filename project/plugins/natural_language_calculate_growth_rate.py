import pandas as pd
from taskweaver.plugin import Plugin, register_plugin
from taskweaver.api.table_info import ResSmartBIApi, drop_nan_str


@register_plugin
class NaturalLanguageCalculateGrowthRate(Plugin):
    def __call__(self, question: str):
        token: str = ResSmartBIApi(
            url="http://10.10.35.110:9070/aiweb/api/v1/login",
            data={"userName": "hcy", "password": "hcy"}
        ).get_token()
        data = {
            "token": token,
            "txt": question,
            "themeId": "I8a8aa3ed018e64106410cbef018e758418f700a6"
        }
        url = "http://10.10.35.110:9070/aiweb/integration/api/v1/query"
        html = ResSmartBIApi(url=url, data=data).get_html()
        df = pd.read_html(html)
        new_df = df[0]
        new_df.columns = new_df.iloc[0]
        new_df = new_df.drop(0)
        new_df = new_df[new_df.apply(drop_nan_str, axis=1)]
        return new_df

