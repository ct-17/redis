import redis
import json
import re
from datetime import datetime

r = redis.Redis(host='192.168.19.157', port=6379, db=2, password='bbbsf34@45dfdfvbnvbvb3SFKKJgjs',decode_responses=True)


class News:
    datas = []
    vnews = {}

    if len(datas) == 0:
        with open("soha-newspublish.json") as json_file:
            text = json_file.read()
            datas = json.loads(text, strict=False)

    @staticmethod
    def no_accent_vietnamese(s):
        s = re.sub(r'[àáạảãâầấậẩẫăằắặẳẵ]', 'a', s)
        s = re.sub(r'[ÀÁẠẢÃĂẰẮẶẲẴÂẦẤẬẨẪ]', 'A', s)
        s = re.sub(r'[èéẹẻẽêềếệểễ]', 'e', s)
        s = re.sub(r'[ÈÉẸẺẼÊỀẾỆỂỄ]', 'E', s)
        s = re.sub(r'[òóọỏõôồốộổỗơờớợởỡ]', 'o', s)
        s = re.sub(r'[ÒÓỌỎÕÔỒỐỘỔỖƠỜỚỢỞỠ]', 'O', s)
        s = re.sub(r'[ìíịỉĩ]', 'i', s)
        s = re.sub(r'[ÌÍỊỈĨ]', 'I', s)
        s = re.sub(r'[ùúụủũưừứựửữ]', 'u', s)
        s = re.sub(r'[ƯỪỨỰỬỮÙÚỤỦŨ]', 'U', s)
        s = re.sub(r'[ỳýỵỷỹ]', 'y', s)
        s = re.sub(r'[ỲÝỴỶỸ]', 'Y', s)
        s = re.sub(r'[Đ]', 'D', s)
        s = re.sub(r'[đ]', 'd', s)
        s = re.sub(r'[ ]', '-', s)
        s = re.sub(r'[,;./]', '-', s)
        s = re.sub(r'[ ̉ ́ ̀ :"()”̣ ]', '', s)
        s = re.sub(r"['‘?!%’̃ ]", '', s)
        return s

    @staticmethod
    def remove_duplyates(text):
        text_new = ''
        text_old = text.lower()
        for t in range(0, len(text_old)):
            if t + 1 < len(text_old):
                if text_old[t] == text_old[t + 1]:
                    pass
                else:
                    text_new += str(text_old[t])
            elif t + 1 == len(text_old):
                if text_old[t] in ["-"]:
                    pass
                else:
                    text_new += str(text_old[t])
        if text_new[0] == "-":
            text_new = text_new[1:]
        return text_new

    @staticmethod
    def load_new_publish(obj_publish):
        key = "newspublish:newsid" + str(obj_publish["NewsId"])
        r.set(key, str(obj_publish))

    @staticmethod
    def load_news_in_zone(obj_publish):
        key = "newsinzone:zone" + str(obj_publish["ZoneId"])
        value = "newspublish:newsid" + str(obj_publish["NewsId"])
        score = obj_publish["PublishedDate"]
        r.zadd(key, {value: score})
        r.zadd("newsinzone:zone0", {value: score})

    @staticmethod
    def load_news_in_zone_by_type(obj_publish):
        key = "newsinzonebytype:zone" + str(obj_publish["ZoneId"]) + "type" + str(obj_publish["Type"])
        value = "newspublish:newsid" + str(obj_publish["NewsId"])
        score = obj_publish["PublishedDate"]
        r.zadd(key, {value: score})

    @staticmethod
    def load_news_in_zone_is_focus(obj_publish):
        if obj_publish["IsFocus"]:
            key = "newsinzoneisfocus:zone" + str(obj_publish["ZoneId"])
            value = "newspublish:newsid" + str(obj_publish["NewsId"])
            score = obj_publish["PublishedDate"]
            r.zadd(key, {value: score})
            r.zadd("newsinzoneisfocus:zone0", {value: score})

    @staticmethod
    def load_news_in_zone_is_on_home(obj_publish):
        if obj_publish["IsOnHome"]:
            key = "newsinzoneisonhome:zone" + str(obj_publish["ZoneId"])
            value = "newspublish:newsid" + str(obj_publish["NewsId"])
            score = obj_publish["PublishedDate"]
            r.zadd(key, {value: score})
            r.zadd("newsinzoneisonhome:zone0", {value: score})

    @staticmethod
    def load_tag_news(obj_publish):
        if obj_publish["TagItem"]:
            text = News.no_accent_vietnamese(obj_publish["TagItem"])
            text_new = News.remove_duplyates(text)
            key = "tagnews:tagurl" + text_new
            value = "newspublish:newsid" + str(obj_publish["NewsId"])
            score = obj_publish["PublishedDate"]
            r.zadd(key, {value: score})

    @staticmethod
    def load_thread_news(obj_publish):
        if obj_publish["ThreadId"] != 0:
            key = "threadnews:threadid" + str(obj_publish["ThreadId"])
            value = "newspublish:newsid" + str(obj_publish["NewsId"])
            score = obj_publish["PublishedDate"]
            r.zadd(key, {value: score})

    @staticmethod
    def load_new_publish_by_tag_item_and_type(obj_publish):
        if "apple" in obj_publish["TagItem"].lower().split(";") and obj_publish["Type"] == 29:
            key = "newspublishbyitemandtype:newsid" + str(obj_publish["NewsId"])
            value = str(obj_publish)
            r.set(str(key), str(value))

    @staticmethod
    def load_news_in_zone_by_last_modified_date(obj_publish):
        key = "newsinzonebylastmodifieddate:zone" + str(obj_publish["ZoneId"])
        value = "newspublish:newsid" + str(obj_publish["NewsId"])
        try:
            text_to_date = datetime.strptime(obj_publish["LastModifiedDate"], '%Y-%m-%dT%H:%M:%S.%f')
        except:
            text_to_date = datetime.strptime(obj_publish["LastModifiedDate"], '%Y-%m-%dT%H:%M:%S')
        date_to_number = int(text_to_date.timestamp()*1000)
        score = date_to_number
        r.zadd(key, {value: score})
        r.zadd("newsinzonebylastmodifieddate:zone0", {value: score})

    def run_all(self):
        for it in self.datas:
            self.load_new_publish(it)
            self.load_news_in_zone(it)
            self.load_news_in_zone_by_type(it)
            self.load_news_in_zone_is_focus(it)
            self.load_news_in_zone_is_on_home(it)
            self.load_tag_news(it)
            self.load_thread_news(it)
            self.load_new_publish_by_tag_item_and_type(it)
            self.load_news_in_zone_by_last_modified_date(it)


if __name__ == "__main__":
    News().run_all()
    # News.load_new_publish(News.datas[0])