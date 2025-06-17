import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from email.mime.text import MIMEText
import os

class Email:
    def send_email(self,sender_email,password,receiver_email,subject,body,filenames):
        #by smtp
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = receiver_email
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))  ##name of attachment only eng
        
        if filenames is None:
            filenames = []
        elif isinstance(filenames, str):
            filenames = [filenames]

        for filename in filenames:
            if os.path.isfile(filename):
                with open(filename, 'rb') as attachment:
                    part = MIMEBase('application', 'vnd.openxmlformats-officedocument.wordprocessingml.document') ##octet-stream
                    part.set_payload(attachment.read())
                    encoders.encode_base64(part)
                    part.add_header('Content-Disposition', f'attachment; filename={os.path.basename(filename)}')
                    msg.attach(part)
            else:
                print(f"File {filename} does not exist.")

        try:
            smtp_server = "twmail.deltaww.com"
            server = smtplib.SMTP(smtp_server)
            server.starttls()
            server.login(sender_email, password)
            server.sendmail(sender_email, receiver_email, msg.as_string())
            print("Email sent successfully!")
        except Exception as e:
            print(f"Failed to send email. Error: {str(e)}")
        finally:
            server.quit()
