def inverted_index(documents):
    index = {}
    for doc_id, doc in enumerate(documents):
        for word in doc.split():
            if word not in index:
                index[word] = []
            if doc_id not in index[word]:
                index[word].append(doc_id)
    return index
