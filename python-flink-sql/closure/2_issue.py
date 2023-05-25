def my_func(*args):
    fs = []
    for i in range(3):
        # 在返回闭包列表fs之前for循环的变量的值已经发生改变了，而且这个改变会影响到所有引用它的内部定义的函数
        def func():
            return i * i

        fs.append(func)
    return fs


fs1, fs2, fs3 = my_func()
print(fs1())
print(fs2())
print(fs3())
