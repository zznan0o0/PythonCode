import xlsxwriter

class XlsxWriter:
  def writeXlsx(self, file_name, title, title_key, data):
    workbook = xlsxwriter.Workbook(file_name)
    worksheet = workbook.add_worksheet()

    data_arr = [title]
    for v in data:
      arr = []
      for k_v in title_key:
        arr.append(v[k_v])
      data_arr.append(arr)
    
    self.xlsxWirtes(worksheet, data_arr)
    workbook.close()
  
  def xlsxWirtes(self, worksheet, data):
    for index1, v1 in enumerate(data):
      for index2, v2 in enumerate(v1):
        worksheet.write(index1, index2, v2)
    return worksheet


if __name__ == '__main__':
  xlsxWriter = XlsxWriter()
  d = [
    {'a1':1, 'a2':2, 'a3':3},
    {'a1':14, 'a2':24, 'a3':34},
  ]
  xlsxWriter.writeXlsx('aaa.xlsx', ['a1', 'a2', 'a3'], ['a1', 'a2', 'a3'], d)