import pymysql
import requests, sys
from bs4 import BeautifulSoup
import ssl

"""
CREATE DATABASE spider;
CREATE USER 'spiderman'@'%' IDENTIFIED BY 'spiderman';
GRANT ALL PRIVILEGES ON spider.* TO 'spiderman'@'%' IDENTIFIED BY 'spiderman' WITH GRANT OPTION;
use spider;
CREATE TABLE `hgsj_magnet` (
  `id` int(10) NOT NULL auto_increment COMMENT 'id',
  `zz_name` varchar(3000) default '' COMMENT '标题',
  `zz_magnet` varchar(3000) default NULL COMMENT '链接',
  `zz_images` varchar(3000) default NULL COMMENT '图片',
  PRIMARY KEY  (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
DROP table hgsj_magnet;
"""
dbname = 'spider'   # 数据库名字
dbhost = 'xxxxxxxxxxx'   # 数据库地址
dbuser = 'spiderman'   # mysql用户名
dbpass = 'spiderman'   # mysql用户名密码
dbport = 3306
dbconn = pymysql.connect(db=dbname, host=dbhost, port=dbport, user=dbuser, password=dbpass)
cursor = dbconn.cursor()

## 导入 ssl 模块，可解决爬取过程中，https 证书报错
ssl._create_default_https_context = ssl._create_unverified_context
requests.adapters.DEFAULT_RETRIES = 15
## 导入 urllib3 模块，默认urllib3不进行HTTPS请求验证，即不认证服务器的证书。
requests.urllib3.disable_warnings()


class download_magnet():

    def __init__(self):
        self.server = "https://qwewqeq.xyz/"
        self.names = []
        self.urls = []
        self.imagess = []


    def get_download_url(self):
        # 这里84是指这个网站爬取的网页有83页，range(1,84)是从1开始到83页
        for i in range(1,3):
            s = requests.session()
            s.proxies = {'https': 'socks5h://127.0.0.1:9527'}
            s.headers = {
                'User-Agent': 'Mozilla/5.0(Wimdows NT 6.1; WOW64) AppleWebkit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36'
            }
            target = "https://www.qwewqeq.xyz/forum.php?mod=forumdisplay&fid=107&typeid=593&filter=typeid&typeid=593&page=" + str(i)

            req = s.get(target, timeout=10, verify=False).text
            html = BeautifulSoup(req, 'lxml')
            bf_th = html.find_all("th",  class_="new")
            for i in bf_th:
                html = BeautifulSoup(str(i), 'lxml')
                a = html.find_all("a", class_="s xst")
                self.names.append(a[0].string)
                self.urls.append(self.server + a[0].get("href")) 
                # return self.urls
            s.close()


    def get_contexts(self, url):
        s = requests.session()
        s.proxies = {'https': 'socks5h://127.0.0.1:9527'}
        s.headers = {
            'User-Agent': 'Mozilla/5.0(Wimdows NT 6.1; WOW64) AppleWebkit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36'
        }
        req = s.get(url,verify=False, timeout=20).text
        html = BeautifulSoup(req, "lxml")
        bf_images = html.find_all("img", class_="zoom")
        bf_magnet = html.find_all("ol")
        if len(bf_images) == 0 or len(bf_magnet) == 0:
            return
        else:
            # 获取图片
            self.imagess.append(bf_images[0].get("file"))
            # 获取磁力链接
            magnet = bf_magnet[0].text
            return magnet
        s.close()



if __name__ == "__main__":
    dl = download_magnet()
    print("正在下载网址。。。请稍后！")
    dl.get_download_url()
    print("正在下载")
    for i in range(int(len(dl.urls))):
        if dl.names[i] != None and dl.get_contexts(dl.urls[i]) != None and dl.imagess[i] != None:
            sql = "INSERT INTO hgsj_magnet(zz_name, zz_magnet, zz_images) VALUES ('%s', '%s', '%s')" % (dl.names[i], dl.get_contexts(dl.urls[i]), dl.imagess[i])
            try:
                # 执行sql语句
                cursor.execute(sql)
                # 提交到数据库执行
                dbconn.commit()
            except:
                # 如果发生错误则回滚
                dbconn.rollback()
        sys.stdout.write("已下载： %.2f%% " % (float(i / int(len(dl.urls))) * 100) + '\r')
        sys.stdout.flush()
    print("下载完成")
    dbconn.close()

