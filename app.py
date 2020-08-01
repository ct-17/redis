from flask import request, jsonify, Flask, abort
import redis

app = Flask(__name__)
app.config['SECRET_KEY'] = 'rerfwefvsdf34534524fsdfser34r23'

r = redis.Redis(host='192.168.19.157', port=6379, db=2, password='bbbsf34@45dfdfvbnvbvb3SFKKJgjs', decode_responses=True)
@app.route('/listidnewspublish', methods=['GET'])
def list_publish():
    # headers = request.headers
    # auth = headers.get("x-api-key")
    listnew = []
    for k in r.keys():
        if "newspublish:" in k:
            listnew.append(int(k[18:]))
    return "{}".format(listnew)

@app.route('/newspublish/<key>', methods=['GET'])
def detail_publish(key):
    k = "newspublish:newsid{}".format(key)
    listnew = r.get(k)
    if request.method == 'GET':
        return "{}".format(listnew)

@app.route('/listidnewsinzone', methods=['GET'])
def list_in_zone():
    listnew = []
    for k in r.keys():
        if "newsinzone:" in k:
            listnew.append(int(k[15:]))
            # listnew.append(r.zrange(k, 0, r.zcard(k)))
    return "{}".format(listnew)

@app.route('/newsinzone/<key>', methods=['GET'])
def detail_in_zone(key):
    k = "newsinzone:zone{}".format(key)
    listnew = r.zrange(k, 0, r.zcard(k))
    if request.method == 'GET':
        return "{}".format(listnew)

@app.route('/listidnewsinzonebytype', methods=['GET'])
def list_in_zone_by_type():
    listnew = []
    for k in r.keys():
        if "newsinzonebytype:" in k:
            listnew.append(k[17:])
    return "{}".format(listnew)

@app.route('/newsinzonebytype/<key>', methods=['GET'])
def detail_in_zone_by_type(key):
    k = "newsinzonebytype:{}".format(key)
    listnew = r.zrange(k, 0, r.zcard(k))
    if request.method == 'GET':
        return "{}".format(listnew)

@app.route('/listidnewsinzoneisfocus', methods=['GET'])
def list_news_in_zone_is_focus():
    listnew = []
    for k in r.keys():
        if "newsinzoneisfocus:" in k:
            listnew.append(int(k[22:]))
    return "{}".format(listnew)

@app.route('/newsinzoneisfocus/<key>', methods=['GET'])
def detail_news_inzone_is_focus(key):
    k = "newsinzoneisfocus:zone{}".format(key)
    listnew = r.zrange(k, 0, r.zcard(k))
    if request.method == 'GET':
        return "{}".format(listnew)

@app.route('/listidnewsinzoneisonhome', methods=['GET'])
def list_news_in_zone_is_on_home():
    listnew = []
    for k in r.keys():
        if "newsinzoneisonhome:" in k:
            listnew.append(int(k[23:]))
    return "{}".format(listnew)

@app.route('/newsinzoneisonhome/<key>', methods=['GET'])
def detail_news_in_zone_is_on_home(key):
    k = "newsinzoneisonhome:zone{}".format(key)
    listnew = r.zrange(k, 0, r.zcard(k))
    if request.method == 'GET':
        return "{}".format(listnew)

@app.route('/listidtagnews', methods=['GET'])
def list_tag_news():
    listnew = []
    for k in r.keys():
        if "tagnews:" in k:
            listnew.append(k[14:])
    return "{}".format(listnew)

@app.route('/tagnews/<key>', methods=['GET'])
def detail_tag_news(key):
    k = "tagnews:tagurl{}".format(key)
    listnew = r.zrange(k, 0, r.zcard(k))
    if request.method == 'GET':
        return "{}".format(listnew)

@app.route('/listidthreadnews', methods=['GET'])
def list_thread_news():
    listnew = []
    for k in r.keys():
        if "threadnews:" in k:
            listnew.append(k[11:])
    return "{}".format(listnew)

@app.route('/threadnews/<key>', methods=['GET'])
def detail_thread_news(key):
    k = "threadnews:{}".format(key)
    listnew = r.zrange(k, 0, r.zcard(k))
    if request.method == 'GET':
        return "{}".format(listnew)

@app.route('/listidnewspublishbyitemandtype', methods=['GET'])
def list_publish_by_item_and_type():
    listnew = []
    for k in r.keys():
        if "newspublishbyitemandtype:" in k:
            listnew.append(int(k[31:]))
    return "{}".format(listnew)

@app.route('/newspublishbyitemandtype/<key>', methods=['GET'])
def detail_publish_by_item_and_type(key):
    k = "newspublishbyitemandtype:newsid{}".format(key)
    listnew = r.get(k)
    if request.method == 'GET':
        return "{}".format(listnew)

@app.route('/listidnewsinzonebylastmodifieddate', methods=['GET'])
def list_news_in_zone_by_last_modified_date():
    listnew = []
    for k in r.keys():
        if "newsinzonebylastmodifieddate:" in k:
            listnew.append(k[29:])
    return "{}".format(listnew)

@app.route('/newsinzonebylastmodifieddate/<key>', methods=['GET'])
def detail_news_in_zone_by_last_modified_date(key):
    k = "newsinzonebylastmodifieddate:{}".format(key)
    listnew = r.zrange(k, 0, r.zcard(k))
    if request.method == 'GET':
        return "{}".format(listnew)

if __name__ == "__main__":
    app.run(debug=True)