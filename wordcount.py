def word_count(words):
    count = {}
    for word in words:
        if word not in count:
            count[word] = 0
        count[word] += 1
    return count
