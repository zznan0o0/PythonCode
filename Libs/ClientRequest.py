import urllib.request
import urllib.parse
import json

class ClientRequest:
  def __init__(self):
    self.__ApiUserId = 'U548163'
    self.__token = 'yYkf3y1P7CeRV3i9C89cMXgjpq3aN0Qt'
    self.__host = 'http://52.83.191.213:8081/'


  def url(self, url):
    return self.__host + url

  def mergeToken(self, submit_data):
    submit_data['ApiUserId'] = self.__ApiUserId
    submit_data['token'] = self.__token
    return submit_data

  def post(self, url, submit_data):
    submit_data = urllib.parse.urlencode(submit_data)
    submit_data = submit_data.encode('utf-8')
    request = urllib.request.Request(url)
    request.add_header("Content-Type","application/x-www-form-urlencoded;charset=utf-8")
    res = urllib.request.urlopen(request, submit_data)
    return res.read().decode('utf-8')
  
  def postJson(self, url, submit_data):
    submit_data = json.dumps(submit_data)
    submit_data = bytes(submit_data, 'utf8')
    request = urllib.request.Request(url)
    request.add_header("Content-Type","application/json;charset=utf-8")
    res = urllib.request.urlopen(request, submit_data)
    return res.read().decode('utf-8')
  
  def postGo(self, url, submit_data):
    url = self.url(url)
    submit_data = self.mergeToken(submit_data)
    return self.post(url, submit_data)

  def postXML(self, url, submit_data=''):
    
    request = urllib.request.Request(url)
    request.add_header("Content-Type","application/xml;charset=utf-8")
    submit_data = submit_data.replace('\r', '').replace('\n', '')
    res = urllib.request.urlopen(request, submit_data.encode('utf-8'))
    res = res.read().decode('utf-8')

    return res

if __name__ == '__main__':
  clientRequest =  ClientRequest()
  print(clientRequest.postGo('v1/Dingding/SendText', {'touser': '091716111036380986', 'text': 'aaa'}))
