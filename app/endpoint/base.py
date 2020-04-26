def str_to_bool(value):
    if value.lower() in ['true', '1', 'yes', 'y', 't', 'on']:
        return True
    return False

def get_request_args(path):
    r_value = {}
    if not '?' in path:
        return r_value
    left_v = path.split('?')[-1]

    data=left_v.split('&')
    print(data)
    for para in data:
        key, value = para.split('=')
        r_value[key.strip()] = value.strip()
    return r_value


def function_convert_to_value(function_convert, value):
    if function_convert == bool:
        return str_to_bool(value)
    return function_convert(value)

def get_request_arg(request, key, func_convert=str):
    args = get_request_args(request._message.path)
    values = args.get(key, None)
    return values