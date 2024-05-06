import time
from taskweaver.plugin import Plugin, register_plugin
from taskweaver.api.table_info import ResSmartBIApi, SmallTable, drop_nan_str
from taskweaver.api.gen_mdx import SqlJsonToMDXConverter
from taskweaver.api.smartbi_utils import query_dataframe_from_mdx


def build_id2content(small_table) -> dict:
    id2content = {}
    for line in small_table.dimension + small_table.measure:
        id2content[line['id']] = line
    return id2content


@register_plugin
class NaturalLanguageQueryData(Plugin):
    def __call__(self, question: str):

        token: str = ResSmartBIApi(
            url="http://10.10.35.110:9070/aiweb/api/v1/login",
            data={"userName": "hcy", "password": "hcy"}
        ).get_token()
        time.sleep(0.1)
        model_id = "I8a8aa3ed018e64106410cbef018e758418f700a6"
        st = SmallTable(question, token, model_id)
        small_table = st.get_small_table_obj()
        nl2sql = st.res.get_nl2sql()
        id2content = build_id2content(small_table)
        mdx = SqlJsonToMDXConverter(id2content, nl2sql).to_converter()
        df = query_dataframe_from_mdx("http://10.10.35.110:9070", "admin", "admin",
                                      model_id, mdx)

        df = df[df.apply(drop_nan_str, axis=1)]
        return df


if __name__ == "__main__":
    from taskweaver.plugin.context import temp_context

    with temp_context() as temp_ctx:
        render = NaturalLanguageQueryData(name="natural_language_query_data", ctx=temp_ctx, config={})

