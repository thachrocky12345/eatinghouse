@defer.inlineCallbacks
def defer_list(dlist, **kwargs):
    result = []
    for item in dlist:
        item = yield item
        result.append(item)
    defer.returnValue(result)