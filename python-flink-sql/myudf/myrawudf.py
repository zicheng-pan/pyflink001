from pyflink.table import DataTypes
from pyflink.table.udf import udf


def myudf(func):
    udf(lambda i:i.upper(), [DataTypes.STRING()], DataTypes.STRING())