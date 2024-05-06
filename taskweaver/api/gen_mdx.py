

def is_date_named_set(min_time_date, date_condition):
    if not min_time_date:
        return False
    date_condition_id = [item["id"] for item in date_condition]
    if min_time_date not in date_condition_id:
        return True
    return False


class SqlJsonToMDXConverter:

    def __init__(self, id2content: dict, sql_json: dict):
        self.sql_json = sql_json
        self.id2content = id2content
        self.row_and_col = self.sql_json.get("row", []) + self.sql_json.get("col", [])

    def has_date_in_row_and_col(self):
        for _id in self.row_and_col:
            if self.id2content.get(_id,{}).get("timeLevel", ""):
                return True

    def build_name_set(self):
        pass

    def build_custom_metric(self):
        pass

    def build_condition(self, used_dim, row: list):
        where = []
        date_mdx, is_where, used_dim = self.build_date_mdx(used_dim)
        if date_mdx:
            # if is_where:
            #     where.append(date_mdx)
            # else:
            row.append(date_mdx)
        for _id, op, value in self.sql_json.get("conds", []):
            if _id not in used_dim:
                content = self.id2content.get(_id)
                if content["is_level"]:
                    dim_name, level_name = content["objectName"], content["name"]
                else:
                    dim_name, level_name = content["name"], content["name"]
                if isinstance(value, list):
                    _member_mdx = [ f"[{dim_name}].[{level_name}].[{member}]" for member in value]
                    _member_mdx = "{" + ",".join(_member_mdx) + "}"
                else:
                    _member_mdx = f"[{dim_name}].[{level_name}].[{value}]"
                row.append(_member_mdx)
                used_dim.append(_id)
        return where, row, used_dim

    def process_order_by(self, dim_name, level_name):
        orders = self.sql_json.get("order_by", [])
        measure_name = orders[-1][-1]
        sort_type = "BDESC" if "DESC" in orders[-1] else "BASC"
        return f"order(Distinct([{dim_name}].[{level_name}].Members), [Measures].[{measure_name}], {sort_type})"

    def build_date_mdx(self, used_dim: list):
        date_condition, is_where = [], True
        has_date = self.has_date_in_row_and_col()
        for _id, op, value in self.sql_json.get("conds", []):
            content = self.id2content.get(_id, {})
            if content.get("timeLevel", ""):
                is_where = False if _id in self.row_and_col or has_date else True
                date_condition.append({
                    "id": _id,
                    "name": content.get("name"),
                    "op": op,
                    "value": value,
                    "objectName": content.get("objectName", ""),
                    "content": content
                })
                used_dim.append(_id)


        if not date_condition:
            return "", is_where, used_dim
        date_mdx = []
        min_time_date = ""
        for _id in self.row_and_col:
            if self.id2content.get(_id, {}).get("timeLevel", ""):
                min_time_date = _id
                used_dim.append(_id)
        is_named_set = is_date_named_set(min_time_date, date_condition)
        date_range, dim_level = [], ""
        for item in date_condition:
            value = item.get("value", "")
            op = item["op"]
            if isinstance(value, list):
                mdx = "{" + ",".join([f"[{item['objectName']}].[{item['name']}].[{member}]" for member in value]) + "}"
                if is_named_set:
                    mdx = f"Descendants({mdx}, [{item['objectName']}].[{item['name']}])"
                mdx = f"order({mdx}, ([{item['objectName']}].currentMember.caption), BASC)"
                self.sql_json["order_by"] = []
                return mdx, is_where, used_dim
            elif ">" in op or "<" in op:
                value = f"{value}"
                dim_level = f"[{item['objectName']}].[{item['name']}].members"
                date_range.append(f"(Ancestor([{item['objectName']}].currentMember, [{item['objectName']}].[{item['name']}]).caption {op} '{value}')")
            else:
                date_mdx.append(
                    f"[{item['objectName']}].[{item['name']}].[{item['value']}]"
                )
        if date_range:
            mdx = f"filter({dim_level}, {'and'.join(date_range)})"
            self.sql_json["order_by"] = []
        else:
            mdx = "{" + ",".join(date_mdx) + "}"
        if is_named_set:
            item = self.id2content.get(min_time_date, {})
            mdx = f"Descendants({mdx}, [{item['objectName']}].[{item['name']}])"
            self.sql_json["order_by"] = []
        return mdx, is_where, used_dim

    def build_row(self, row, used_dim):
        has_order = False
        for _id in self.row_and_col:
            if _id not in used_dim:
                content = self.id2content.get(_id)
                dim_name, level_name = content["name"], content["name"]
                # if not has_order and self.sql_json.get("order_by", ""):
                #     row.append(self.process_order_by(dim_name, level_name))
                #     has_order = True
                #     continue
                row.append(f"[{dim_name}].[{level_name}].Members")
        return row

    def to_converter(self):
        used_dim, row = [], []
        col = [f"[Measures].[{self.id2content[_id]['name']}]" for _id in self.sql_json.get("measure", "") if _id in self.id2content]
        where, row, used_dim = self.build_condition(used_dim, row)
        row = self.build_row(row, used_dim)
        mdx = ""
        if col:
            col = "{" + ",".join(col) + "}"
            mdx += f"SELECT\n{col} ON COLUMNS"
        if row:
            mdx += ",\n"
            row = "{" + "*".join(row) + "}"
            # if self.sql_json.get("limit"):
            #     limit = self.sql_json["limit"][0][-1]
            #     mdx += f"NON EMPTY NonEmptySubset({row}, 0, {limit}) ON ROWS\n"
            # else:
            mdx += f"NON EMPTY {row} ON ROWS\n"
        mdx += "\nFROM [cube]\n"
        if where:
            where = "{" + "*".join(where) + "}"
            mdx += f"WHERE {where}"
        return mdx

