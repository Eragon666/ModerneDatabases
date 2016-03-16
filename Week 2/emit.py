def emit(key, value):
    if key in emit_dict:
        emit_dict[key].append(value)
    else:
        emit_dict[key] = [value]