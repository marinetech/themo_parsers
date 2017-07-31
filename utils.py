def clean_str(str):
    ret = str
    ret = ret.replace('[', '')
    ret = ret.replace(']', '')
    ret = ret.replace('(', '')
    ret = ret.replace(')', '')
    ret = ret.replace('{', '')
    ret = ret.replace('}', '')
    return ret


def format_time(str):
    ret = str
    #ret = ret.replace(':', '-')
    ret = ret.split(".")[0]
    return ret
