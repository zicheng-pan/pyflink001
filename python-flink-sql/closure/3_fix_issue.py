def my_func(*args):
    fs = []
    for i in range(3):
        # 结论：返回闭包中不要引用任何循环变量，或者后续会发生变化的变量。
        # 方法：对形参的不同赋值会保留在当前函数定义中，不会对其他函数有影响。
        def func(_i = i):
            return _i * _i
        fs.append(func)
    return fs

fs1, fs2, fs3 = my_func()
print(fs1())
print(fs2())
print(fs3())
