class KHandleData:

    '''
    转化字段名
    @param d array 
    --- eg start ---
    [
        {'k1': 'v1'}
    ]
    --- eg end ---
    @param ks array
    --- eg start ---
    [['k3', 'k1', 'v'], ['k4', 'k2', '']]
    --- eg end ---

    @return array
    --- eg start ---
    [{'k3': 'v3', 'k4': ''}]
    --- eg end ---
    '''
    def convertFiled(self, d, ks):
        arr = []
        for v in d:
            dic = {}
            for kv in ks:
                dic[kv[0]] = self.getVal(v, kv[1], kv[2]) 
            
            arr.append(dic)
        
        return arr

    '''
    转化没有或者None的字段
    @param d array 
    --- eg start ---
    [
        {'k1': None}
    ]
    --- eg end ---
    @param ks array
    --- eg start ---
    [['k1', 'v1'], ['k2', 'v2']]
    --- eg end ---

    @return array
    --- eg start ---
    [{'k1': 'v1', 'k2': 'k2'}]
    --- eg end ---
    '''
    
    def setDefaultVal(self, d, ks):
        arr = []
        for v in d:
            dic = {}
            for kv in ks:
                dic[kv[0]] = self.getVal(v, kv[0], kv[1]) 
                if dic[kv[0]] == None:
                    dic[kv[0]] = kv[1]

            arr.append(dic)
        
        return arr

    def distributePropsArray(self, d1, d2, k1, k2, p):
        def fn(v1, v2, k):
            for pv in p:
                v1[pv[0]] = self.getVal(v2, pv[1], pv[2])

            return v1

        return self.mapDictDict(d1, d2, k1, k2, fn)

    def mapDicstDicts(self, d1, d2, k1, k2, fn):
        dict1 = self.convertDicts(d1, k1)
        dict2 = self.convertDicts(d2, k2)
        arr = []
        for key, v in dict1.items():
            arr.append(fn(v, self.getVal(dict2, key, []), key))

        return arr

    def mapDictDicts(self, d1, d2, k1, k2, fn):
        dict2 = self.convertDicts(d2, k2)
        arr = []
        for v in d1:
            key = self.convertKey(v, k1)
            arr.append(fn(v, self.getVal(dict2, key, []), key))

        return arr

    def mapDictDict(self, d1, d2, k1, k2, fn):
        dict2 = self.convertDict(d2, k2)
        arr = []
        for v in d1:
            key = self.convertKey(v, k1)
            arr.append(fn(v, self.getVal(dict2, key, {}), key))

        return arr

    def mapDicts(self, d, key, fn):
        dic = self.convertDicts(d, key)
        arr = []
        for k, v in dic.items():
            arr.append(fn(v, k))
        return arr

    def convertDicts(self, d, k):
        dic = {}
        for v in d:
            key = self.convertKey(v, k)
            dic = self.setVals(dic, key, v)

        return dic

    def convertDict(self, d, k):
        dic = {}
        for v in d:
            key = self.convertKey(v, k)
            dic[key] = v

        return dic

    def convertKey(self, d, k):
        arr = []
        for kv in k:
            arr.append(str(self.getVal(d, kv, '')))

        return ','.join(arr)

    def setVals(self, d, k, v):
        if k not in d.keys():
            d[k] = []

        d[k].append(v)
        return d

    def getVals(self, d, k, intval=''):
        return [self.getVal(v, k, intval) for v in d]

    def getVal(self, d, k, intval=''):
        return intval if k not in d.keys() else d[k]

    def getDictsVal(self, d, k, intval=''):
        arr = []
        for v in d:
            arr.append(intval if k not in v.keys() else v[k])

        return arr


if __name__ == '__main__':
    kHandleData = KHandleData()

    d1 = [{'k': "a", 'v': 2}, {'k': "b", 'v': 3}, {'k': "b", 'v': 24}]
    d2 = [{'k': 1, 'c': 3}, {'k': 2, 'c': 4}]

    def fn(v, k):
        for vv in v:
            vv['ccc'] = 0

        return v

    c = kHandleData.mapDicts(d1, ['k'], fn)

    print(c)
