import urllib.request
import urllib.parse
import json

class ClientRequest:
  def __init__(self):
    self.__ApiUserId = '111'
    self.__token = '111'
    self.__host = 'http://0.0.0.0:9091/'


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
