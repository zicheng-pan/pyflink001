class MyUDF:
    def __init__(self):

        self.dic = {}

    def udf(self, func, name=None):
        if name:
            self.dic[name] = func
        else:
            self.dic[func.__name__] = func


if __name__ == '__main__':
    my = MyUDF()
    my.udf(lambda x:x.upper(),"upper")
    print(my.dic)