def ii_map(file_path):
    result = {}
    with open(file_path, 'r') as f:
        for line in f:
            words = line.strip().split()
            for word in words:
                if word not in result:
                    result[word] = []
                if file_path not in result[word]:
                    result[word].append(file_path)
    return result
