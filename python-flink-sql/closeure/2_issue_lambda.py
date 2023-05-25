def my_func(*args):
    fs = []
    for i in range(3):
        func = lambda: i * i
        fs.append(func)
    return fs


fs1, fs2, fs3 = my_func()
print(fs1())
print(fs2())
print(fs3())
