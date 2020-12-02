import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from email.header import Header
 

class HandleEmail:
    def __init__(self, email_host, email_port, email_user, email_pass, sender, to_receiver, cc_receiver=[]):
        self.__config = {
            'email_host': email_host,
            'email_port': email_port,
            'email_user': email_user,
            'email_pass': email_pass,
            'sender': sender,
            'to_receiver': to_receiver,
            'cc_receiver': cc_receiver,
        }

        self.smtpObj = smtplib.SMTP() 
        
    
    def setReceiver(self, to_receiver, cc_receiver=[]):
        self.__config['to_receiver'] = to_receiver
        self.__config['cc_receiver'] = cc_receiver

    def connect(self):
        self.smtpObj.connect(self.__config['email_host'], self.__config['email_port'])
        self.smtpObj.login(self.__config['email_user'], self.__config['email_pass'])

    def sendEmail(self, title, content):
        message = MIMEMultipart()
        message['From'] = self.__config['sender']
        message['To'] = ';'.join(self.__config['to_receiver'])
        message['Cc'] = ';'.join(self.__config['cc_receiver'])
        message['Subject'] = Header(title)
        message.attach(MIMEText(content, 'plain', 'utf-8'))

        try:
            receivers = self.__config['to_receiver'] + self.__config['cc_receiver']
            self.connect()
            self.smtpObj.sendmail(self.__config['sender'], receivers, message.as_string())
            self.smtpObj.quit()
            return {'code': 1, 'msg': 'ok'}
        except smtplib.SMTPException as e :
            return {'code': 0, 'msg': str(e)}
        

if __name__ == "__main__":
    email_host = 'smtp.163.com'
    email_user = 'dx0w0xb@163.com'
    email_pass = 'KPEDAIPQCSFZCONF'
    
    sender = 'dx0w0xb@163.com'
    to_receiver = ['dx0w0xb@outlook.com'] # 收件人
    cc_receiver = ['517701884@qq.com'] # 抄送
    receivers = to_receiver + cc_receiver
    
    content = 'python自动发送的带压缩包的电子邮件。'
    title = 'python发送邮件测试'

    handleEmail = HandleEmail(email_host, '25', email_user, email_pass, sender, to_receiver, cc_receiver)
    print(handleEmail.sendEmail("code", 'hello world !!!'))