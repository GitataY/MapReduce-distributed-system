def wc_map(input_str):
    words = input_str.strip().split()
    result = {}
    for word in words:
        result[word] = 1
    return result
