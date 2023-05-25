# 求加法
def func_dec(func):
    def wrapper(*args):
        if len(args) == 2:
            func(*args)
        else:
            print('Error! Arguments = %s' % list(args))

    return wrapper


@func_dec
def add_sum(*args):
    print(sum(args))


# add_sum = func_dec(add_sum)
args = range(1, 3)
add_sum(*args)


# 字符串转大写
def func_uppercase(func):
    def wrapper(content):
        return func(str(content).upper())

    return wrapper


@func_uppercase
def generate_name(name):
    return {"name": name}


result = generate_name("xiaoli")
print(result)
