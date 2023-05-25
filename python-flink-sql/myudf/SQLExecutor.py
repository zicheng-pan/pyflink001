import myudf

udfobj = myudf.MyUDF()


def trime(s, value):
    content = s.split(value)
    return content[0] + content[1]


class SQLExecutor():

    def __init__(self, myudf):
        self.udf = myudf

    def exec(self, t_env, sql):
        result = []

        for key in self.udf.dic:
            print(key)
            sql = str(sql)
            if sql.__contains__(key):
                raw_sql = trime(sql, key)
                raw_sql = trime(raw_sql, "(")
                raw_sql = trime(raw_sql, ")")

                print("execute sql" + raw_sql)
                raw_result = ["xiaoli", "xiaoming"]
                # result = t_env.execute_sql(raw_sql).wait()
                func = udfobj.dic[key]
                for i in raw_result:
                    result.append(func(i))
        return result


@udfobj.udf
def upper(x):
    return x.upper()


if __name__ == '__main__':
    sqlexecutor = SQLExecutor(udfobj)
    sql = "SELECT upper(b) as b FROM mySource"
    return_result = sqlexecutor.exec(None, sql)
    print(return_result)