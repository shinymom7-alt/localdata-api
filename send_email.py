import smtplib, os, sys
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.header import Header
from email import encoders
from datetime import datetime

def send_excel_email(filepath):
    gmail_user     = os.environ["GMAIL_USER"]
    gmail_password = os.environ["GMAIL_PASSWORD"]
    gmail_to       = os.environ["GMAIL_TO"]

    # 여러 수신자 처리
    recipients = [r.strip() for r in gmail_to.split(",")]

    filename = os.path.basename(filepath)
    today    = datetime.now().strftime("%Y-%m-%d")

    msg = MIMEMultipart()
    msg["From"]    = gmail_user
    msg["To"]      = ", ".join(recipients)
    msg["Subject"] = f"[인허가정보] 신규가맹점 데이터 {today}"

    body = f"""안녕하세요,

오늘({today}) 자동 추출된 인천 서구/검단구 신규가맹점 데이터를 첨부합니다.

- 파일명: {filename}
- 대상: 인천 서구 (검단구 포함)
- 업종: 병원, 약국, 미용업, 음식점 등 13개 업종
- 조회기간: 최근 7일 변동분
"""
    msg.attach(MIMEText(body, "plain", "utf-8"))

    with open(filepath, "rb") as f:
        part = MIMEBase("application", "octet-stream")
        part.set_payload(f.read())
    encoders.encode_base64(part)

    # 한글 파일명 깨짐 방지
    part.add_header(
        "Content-Disposition",
        "attachment",
        filename=("utf-8", "", filename)
    )
    msg.attach(part)

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(gmail_user, gmail_password)
        server.sendmail(gmail_user, recipients, msg.as_string())

    print(f"✓ 이메일 전송 완료 → {', '.join(recipients)}")

if __name__ == "__main__":
    send_excel_email(sys.argv[1])
