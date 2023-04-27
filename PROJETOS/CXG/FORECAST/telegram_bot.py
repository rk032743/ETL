import requests
from requests.structures import CaseInsensitiveDict

# TOKEN = "5758485761:AAHB4tPdKEnHiJ2LC28mqr4I-Waz2yo-G2M"
# url = f"https://api.telegram.org/bot{TOKEN}/getUpdates"
# print(requests.get(url).json())

def bot_notification(msg):
    TOKEN = "5758485761:AAHB4tPdKEnHiJ2LC28mqr4I-Waz2yo-G2M"
    chat_id = "1065124098"
    message = msg
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={chat_id}&text={message}"
    rsp = requests.get(url).json()

    return


def bot_notification2(msg):
    pass
    TOKEN = "5422458138:AAHQ1SfzEIqySD1Xrna6sxEabdZNgGfPG8c"
    chat_id = "-875504960"
    message = msg
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={chat_id}&text={message}"
    rsp = requests.get(url).json()

    return


def bot_notification_teste():
    TOKEN = "5758485761:AAHB4tPdKEnHiJ2LC28mqr4I-Waz2yo-G2M"
    chat_id = "1065124098"
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={chat_id}"
    headers = CaseInsensitiveDict()
    headers["Content-Type"] = "application/json"
    data = {    "text":
                "ID: 0 \n" \
                "HOST : 172.57.154.123"
                }

    rsp = requests.post(url, headers=headers, json=data)
    print(rsp.status_code)
    return


# if __name__ == '__main__':
#     bot_notification_teste()