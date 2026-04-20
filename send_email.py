import smtplib
import os
import sys
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
from datetime import datetime

def send_excel_email(filepath):
    gmail_user     = os.environ["GMAIL_USER"]
    gmail_password = os.environ["GMAIL_PASSWORD"]
    gmail_to       = os.environ["GMAIL_TO"]

    filename = os.path.basename(filepath)
    today    = datetime.now().strftime("%Y-%m-%d")

    msg = MIMEMultipart()
    msg["From"]    = gmail_user
    msg["To"]      = gmail_to
    msg["Subject"] = f"[인허가정보] 신규가맹점 데이터 {today}"

    body = f"""안녕하세요,

오늘({today}) 자동 추출된 인천 서구/검단구 신규가맹점 데이터를 첨부합니다.

- 파일명: {filename}
- 대상: 인천 서구 (검단구 포함)
- 업종: 병원, 약국, 미용업, 음식점 등 13개 업종
- 조회기간: 최근 7일 변동분

문의사항이 있으시면 알려주세요.
"""
    msg.attach(MIMEText(body, "plain", "utf-8"))

    with open(filepath, "rb") as f:
        part = MIMEBase("application", "octet-stream")
        part.set_payload(f.read())
    encoders.encode_base64(part)
    part.add_header("Content-Disposition", f'attachment; filename="{filename}"')
    msg.attach(part)

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(gmail_user, gmail_password)
        server.sendmail(gmail_user, gmail_to, msg.as_string())

    print(f"✓ 이메일 전송 완료 → {gmail_to}")

if __name__ == "__main__":
    send_excel_email(sys.argv[1])