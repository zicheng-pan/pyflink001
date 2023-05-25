def outer_func():
    loc_list = []

    def inner_func(name):
        loc_list.append(len(loc_list) + 1)
        print('{} loc_list = {}'.format(name, loc_list))

    return inner_func


clo_func_0 = outer_func()
clo_func_0('clo_func_0')
clo_func_0('clo_func_0')
clo_func_0('clo_func_0')
clo_func_1 = outer_func()
clo_func_1('clo_func_1')
clo_func_0('clo_func_0')
clo_func_1('clo_func_1')
