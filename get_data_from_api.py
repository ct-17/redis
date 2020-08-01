from datetime import datetime
import redis
import time
import sys
from urllib.request import urlopen
from convert import News

r = redis.Redis(host='192.168.19.157', port=6379, db=3, password='bbbsf34@45dfdfvbnvbvb3SFKKJgjs', decode_responses=True)
# r = redis.Redis(host='192.168.19.155', port=6379, db=0, password='bmd1eWVuIG1hbmggY3Vvbmc=', decode_responses=True)
link = "http://127.0.0.1:5000"

class GetDataAPI:
    @staticmethod
    def get_data(url):
        data = urlopen(url)
        json_data = eval(data.read().decode("UTF8"))
        return json_data

    @staticmethod
    def save_newspublish(datas):
        key = "newspublish:newsid" + str(datas['NewsId'])
        r.set(key, str(datas))

    @staticmethod
    def save_timer_newspublish(datas):
        key = "newspublishtimer:newsid" + str(datas["NewsId"])
        score = int(datetime.strptime(datas['DistributionDate'], '%Y-%m-%dT%H:%M:%S').timestamp())
        r.zadd(key, {str(datas): score})
        r.zadd("newspublishtimer:newsid0", {str(datas): score})

    @staticmethod
    def save_news_in_zone(datas=None, zone_id=None, news_id=None, score_z=None):
        if datas is not None:
            key = "newsinzone:zone" + str(datas["ZoneId"])
            value = "newspublish:newsid" + str(datas["NewsId"])
            score = datas["PublishedDate"]
        elif zone_id is not None and news_id is not None and score_z is not None:
            key = "newsinzone:zone" + str(zone_id)
            value = "newspublish:newsid" + str(news_id)
            score = score_z
        r.zadd(key, {value: score})
        r.zadd("newsinzone:zone0", {value: score})

    @staticmethod
    def save_news_in_zone_by_type(datas=None, zone_id=None, type_zt=None, news_id=None, score_zt=None):
        if datas is not None:
            key = "newsinzonebytype:zone" + str(datas["ZoneId"]) + "type" + str(datas["Type"])
            value = "newspublish:newsid" + str(datas["NewsId"])
            score = datas["PublishedDate"]
        elif zone_id is not None and type_zt is not None and news_id is not None and score_zt is not None:
            key = "newsinzonebytype:zone" + str(zone_id) + "type" + str(type_zt)
            value = "newspublish:newsid" + str(news_id)
            score = score_zt
        r.zadd(key, {value: score})

    @staticmethod
    def save_news_in_zone_is_focus(datas=None, zone_id=None, news_id=None, score_z=None):
        if datas is not None:
            if datas["IsFocus"]:
                key = "newsinzoneisfocus:zone" + str(datas["ZoneId"])
                value = "newspublish:newsid" + str(datas["NewsId"])
                score = datas["PublishedDate"]
                r.zadd(key, {value: score})
                r.zadd("newsinzoneisfocus:zone0", {value: score})
        elif zone_id is not None and news_id is not None and score_z is not None:
            key = "newsinzoneisfocus:zone" + str(zone_id)
            value = "newspublish:newsid" + str(news_id)
            score = score_z
            r.zadd(key, {value: score})
            r.zadd("newsinzoneisfocus:zone0", {value: score})

    @staticmethod
    def save_news_in_zone_is_on_home(datas=None, zone_id=None, news_id=None, score_z=None):
        if datas is not None:
            if datas["IsOnHome"]:
                key = "newsinzoneisonhome:zone" + str(datas["ZoneId"])
                value = "newspublish:newsid" + str(datas["NewsId"])
                score = datas["PublishedDate"]
                r.zadd(key, {value: score})
                r.zadd("newsinzoneisonhome:zone0", {value: score})
        elif zone_id is not None and news_id is not None and score_z is not None:
            key = "newsinzoneisonhome:zone" + str(zone_id)
            value = "newspublish:newsid" + str(news_id)
            score = score_z
            r.zadd(key, {value: score})
            r.zadd("newsinzoneisonhome:zone0", {value: score})

    @staticmethod
    def save_tag_news(datas=None, tag_id=None, news_id=None, score_z=None):
        if datas is not None:
            if datas["TagItem"]:
                tag = News.no_accent_vietnamese(datas["TagItem"])
                tag_new = News.remove_duplyates(tag)
                key = "tagnews:tagurl" + tag_new
                value = "newspublish:newsid" + str(datas["NewsId"])
                score = datas["PublishedDate"]
                r.zadd(key, {value: score})
        elif tag_id is not None and news_id is not None and score_z is not None:
            key = "tagnews:tagurl" + tag_id
            value = "newspublish:newsid" + str(news_id)
            score = score_z
            r.zadd(key, {value: score})

    @staticmethod
    def save_thread_news(datas=None, thread_id=None, news_id=None, score_z=None):
        if datas is not None:
            if datas["ThreadId"] != 0:
                key = "threadnews:threadid" + str(datas["ThreadId"])
                value = "newspublish:newsid" + str(datas["NewsId"])
                score = datas["PublishedDate"]
                r.zadd(key, {value: score})
        elif thread_id is not None and news_id is not None and score_z is not None:
            key = "threadnews:threadid" + str(thread_id)
            value = "newspublish:newsid" + str(news_id)
            score = score_z
            r.zadd(key, {value: score})

    @staticmethod
    def save_publish_by_item_and_type(datas=None, news_id=None, score_z=None):
        if datas is not None:
            if "apple" in datas["TagItem"].lower().split(";") and datas["Type"] == 29:
                key = "newspublishbyitemandtype:newsid" + str(datas["NewsId"])
                value = "newspublish:newsid" + str(datas["NewsId"])
                score = datas["PublishedDate"]
                r.zadd(key, {value: score})
        elif news_id is not None:
            key = "newspublishbyitemandtype:newsid" + str(news_id)
            value = "newspublish:newsid" + str(news_id)
            score = score_z
            r.zadd(key, {value: score})

    @staticmethod
    def save_news_in_zone_by_last_modified_date(datas=None, zone_id=None, news_id=None, date=None):
        if datas is not None:
            try:
                text_to_date = datetime.strptime(datas["LastModifiedDate"], '%Y-%m-%dT%H:%M:%S.%f')
            except:
                text_to_date = datetime.strptime(datas["LastModifiedDate"], '%Y-%m-%dT%H:%M:%S')
            key = "newsinzonebylastmodifieddate:zone" + str(datas["ZoneId"])
            value = "newspublish:newsid" + str(datas["NewsId"])
            score = int(text_to_date.timestamp() * 1000)
            r.zadd(key, {value: score})
            r.zadd("newsinzonebylastmodifieddate:zone0", {value: score})
        elif zone_id is not None and news_id is not None and date is not None:
            try:
                text_to_date = datetime.strptime(date, '%Y-%m-%dT%H:%M:%S.%f')
            except:
                text_to_date = datetime.strptime(date, '%Y-%m-%dT%H:%M:%S')
            key = "newsinzonebylastmodifieddate:zone" + str(zone_id)
            value = "newspublish:newsid" + str(news_id)
            score = int(text_to_date.timestamp() * 1000)
            r.zadd(key, {value: score})
            r.zadd("newsinzonebylastmodifieddate:zone0", {value: score})

    @staticmethod
    def get_news_publish(datas):
        date = datetime.strptime(datas["DistributionDate"], '%Y-%m-%dT%H:%M:%S')
        if date <= datetime.now().replace(microsecond=0):
            GetDataAPI.save_newspublish(datas)
            GetDataAPI.save_news_in_zone(datas)
            GetDataAPI.save_news_in_zone_by_type(datas)
            GetDataAPI.save_news_in_zone_is_focus(datas)
            GetDataAPI.save_news_in_zone_is_focus(datas)
            GetDataAPI.save_news_in_zone_is_on_home(datas)
            GetDataAPI.save_tag_news(datas)
            GetDataAPI.save_thread_news(datas)
            GetDataAPI.save_publish_by_item_and_type(datas)
            GetDataAPI.save_news_in_zone_by_last_modified_date(datas)
        else:
            GetDataAPI.save_timer_newspublish(datas)

    @staticmethod
    def get_news_in_zone(obj_zone, obj_news_id, obj_score):
        GetDataAPI.save_news_in_zone(zone_id=obj_zone, news_id=obj_news_id, score_z=obj_score)

    @staticmethod
    def get_news_in_zone_by_type(obj_zone, obj_type, obj_news_id, obj_score):
        GetDataAPI.save_news_in_zone_by_type(zone_id=obj_zone, type_zt=obj_type, news_id=obj_news_id, score_zt=obj_score)

    @staticmethod
    def get_news_in_zone_is_focus(obj_zone, obj_news_id, obj_score):
        GetDataAPI.save_news_in_zone(zone_id=obj_zone, news_id=obj_news_id, score_z=obj_score)

    @staticmethod
    def get_news_in_zone_is_on_home(obj_zone, obj_news_id, obj_score):
        GetDataAPI.save_news_in_zone(zone_id=obj_zone, news_id=obj_news_id, score_z=obj_score)

    @staticmethod
    def get_tag_news(obj_tag, obj_news_id, obj_score):
        GetDataAPI.save_tag_news(tag_id=obj_tag, news_id=obj_news_id, score_z=obj_score)

    @staticmethod
    def get_thread_news(obj_thread, obj_news_id, obj_score):
        GetDataAPI.save_thread_news(thread_id=obj_thread, news_id=obj_news_id, score_z=obj_score)

    @staticmethod
    def get_publish_by_item_and_type(obj_news_id, obj_score):
        GetDataAPI.save_publish_by_item_and_type(news_id=obj_news_id, score_z=obj_score)

    @staticmethod
    def get_news_in_zone_by_last_modified_date(obj_zone, obj_news_id):
        datas = GetDataAPI.get_data(link + "/newspublish/" + str(obj_news_id))
        obj_date = datas["LastModifiedDate"]
        GetDataAPI.save_news_in_zone_by_last_modified_date(zone_id=obj_zone, news_id=obj_news_id, date=obj_date)

    @staticmethod
    def get_timer_newspublish():
        while True:
            try:
                r.ping()
                if r.zcard("newspublishtimer:newsid0") > 0:
                    obj_data = r.zrange("newspublishtimer:newsid0", 0, 0)[0]
                    text_to_date = datetime.strptime(eval(obj_data)["DistributionDate"], '%Y-%m-%dT%H:%M:%S')
                    print("Dang bai sau: ", text_to_date - datetime.now().replace(microsecond=0))
                    if text_to_date <= datetime.now().replace(microsecond=0):
                        GetDataAPI.get_news_publish(eval(obj_data))
                        print("===================================================================")
                        print("Add key: newspublish:newsid{}".format(str(eval(obj_data)["NewsId"])))
                        r.zrem("newspublishtimer:newsid0", obj_data)
                        r.delete("newspublishtimer:newsid" + str(eval(obj_data)["NewsId"]))
                        print("Delete key: newspublishtimer:newsid{}".format(str(eval(obj_data)["NewsId"])))
                    time.sleep(1)
                else:
                    time.sleep(60)
            except redis.exceptions.ConnectionError:
                again = 0
                while again < 3:
                    try:
                        print("Try connecting to redis server...")
                        r.ping()
                        print("Connect redis server successfully.")
                        GetDataAPI.get_timer_newspublish()
                    except redis.exceptions.ConnectionError:
                        again += 1
                if again == 3:
                    print("Error connect to server redis.")
                    return False



if __name__ == "__main__":
    if sys.argv[1] == 'help':
        print("==========================================")
        print("python get_data_from_api.py config ")
        print("================ config ==================")
        print("update: Cap nhat danh sach du lieu newpulish.")
        print("timer: su ly hang cho dang bai.")

    if sys.argv[1] == 'update':
        url_p = link + "/listidnewspublish"
        list_data_p = GetDataAPI.get_data(url_p)
        for obj_key in list_data_p:
            link_obj = link + "/newspublish/" + str(obj_key)
            datas = GetDataAPI.get_data(link_obj)
            GetDataAPI.get_news_publish(datas)

    if sys.argv[1] == 'timer':
        GetDataAPI.get_timer_newspublish()

    # datas_p = GetDataAPI.get_data(link + "/newspublish/" + str(obj_news_id))
    # score_p = datas["PublishedDate"]
    # GetDataAPI.get_news_in_zone(obj_zone='321', obj_news_id='20190521173414609', obj_score=score_p)
    # GetDataAPI.get_news_in_zone_by_type(obj_zone='321', obj_type='10', obj_news_id='20190521173414609', obj_score=score_p)
    # GetDataAPI.get_news_in_zone_is_focus(obj_zone='321', obj_news_id='20190521173414609', obj_score=score_p)
    # GetDataAPI.get_news_in_zone_is_on_home(obj_zone='321', obj_news_id='20190521173414609', obj_score=score_p)
    # GetDataAPI.get_tag_news(obj_tag='bai-bao-so-n', obj_news_id='20190521173414609', obj_score=score_p)
    # GetDataAPI.get_thread_news(obj_thread='1102', obj_news_id='20190521173414609', obj_score=score_p)
