def clean_identifiers_quotes(list_of_identifiers):
    clean = lambda x: x.strip('\"').strip("\'")
    result = []
    for identifier in list_of_identifiers:
        if '.' in identifier:
            result.append('.'.join([clean(i) for i in identifier.split('.')]))
            
        else:
            result.append(clean(identifier))
    return result


def nested_rename(dic, keys, new_key):
    for key in keys[:-1]:
        if key in dic:
            dic = dic.setdefault(key, {})
    try:
        dic[new_key] = dic.pop(keys[-1])
    except KeyError:
        pass
    
def getFromDict(dataDict, mapList):    
    for k in mapList: dataDict = dataDict[k]
    return dataDict

def removeFromDict(dataDict, mapList):    
    for key in keys[:-1]:
        if key in dic:
            dic = dic.setdefault(key, {})
    try:
        dic.pop(keys[-1])
    except KeyError:
        pass
        