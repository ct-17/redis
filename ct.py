import redis
import json
from datetime import datetime
r = redis.Redis(host='192.168.19.157', port=6379, db=2, password='bbbsf34@45dfdfvbnvbvb3SFKKJgjs', decode_responses=True)
datas = []

# if len(datas) == 0:
#     with open("soha-newspublish.json") as json_file:
#         text = json_file.read()
#         datas = json.loads(text, strict=False)
#
# def RemoveDuplyates(text):
#     text_new = ''
#     for t in range(0, len(text)):
#         if t+1 < len(text):
#             if text[t] == text[t+1]:
#                 pass
#             else:
#                 text_new += text[t]
#         elif t+1 == len(text):
#             text_new += text[t]
#     return text_new
#
# def sort_data_by_date():
#     # sort_data = datas.sort(key= lambda t:datetime.strptime(t["LastModifiedDate"], '%Y-%m-%dT%H:%M:%S.%f'), reverse=True)
#     # print(datas)
#     # sort_data = sorted(datas, key=lambda t:t["LastModifiedDate"], reverse=True)
#
#     for i in sort_data:
#         print(i["LastModifiedDate"])
#
import turtle
import sys

def ct():
    print("Da chay vao ham nay...")

if __name__ == '__main__':
    # r = redis.Redis(host='192.168.19.157', port=6379, db=2, password='bbbsf34@45dfdfvbnvbvb3SFKKJgjs', decode_responses=True)
    r = redis.Redis(host='192.168.19.155', port=6379, db=0, password='bmd1eWVuIG1hbmggY3Vvbmc=', decode_responses=True)

    i = 1
    while i < 3:
        try:
            r.zcard("newspublishtimer:newsid0")
            ct()
        except:
            i+=1
            print(i)
            print("Connect Error.")
    if i == 3:
        print("Connect False")
