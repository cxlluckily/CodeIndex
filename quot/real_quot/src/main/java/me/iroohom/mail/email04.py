#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020/3/20 10:40
# @Author  : Roohom
# @Site    : 
# @File    : email04.py
# @Software: PyCharm
"""
添加邮件头， 抄送等信息
    mail["From"] 表示发送者信息，包括姓名和邮件
    mail["To"] 表示接收者信息，包括姓名和邮件地址
    mail["Subject"] 表示摘要或者主题信息
"""
from email.mime.text import MIMEText
from email.header import Header

mail_content = """
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <title>Title</title>
        </head>
        <body>
        <h1> Hello world!来自Python自动化邮件发送！</h1>
        </body>
        </html>
        """

msg = MIMEText(mail_content, "html", "utf-8")
# 下面代码故意写错，说明，所谓的发送者的地址，只是从一个Header的第一个参数作为字符串构建的内容
# 用utf8编码是因为很可能内容包含非英文字符
header_from = Header("从董如红的邮箱发出去的<roohom@qq.cn>", "utf-8")
msg['From'] = header_from

# 填写接受者信息
header_to = Header("小拳拳锤你噢", 'utf-8')
msg['To'] = header_to

header_sub = Header("使用Python让工作自动化", 'utf-8')
msg['Subject'] = header_sub


# 发送email地址，此处地址直接使用我的qq邮箱，密码一般需要临时输入，此处偷懒
from_addr = "roohom@qq.com"
# 此处密码是经过申请设置后的授权码，不是不是不是你的qq邮箱密码
from_pwd = "tpocgnmjvnvlbihe"

# 收件人信息
# 构建邮件接受者信息
addr1 = "911066833@qq.com"
addr2 = "1984392149@qq.com"
addr3 = "1451171571@qq.com"
to_addr = [addr1, addr2, addr3]

smtp_srv = "smtp.qq.com"


try:
    import smtplib

    srv = smtplib.SMTP_SSL(smtp_srv.encode(), 465)

    srv.login(from_addr, from_pwd)
    srv.sendmail(from_addr, to_addr, msg.as_string())
    srv.quit()
    print("邮件发送成功!")
except Exception as e:
    print(e)