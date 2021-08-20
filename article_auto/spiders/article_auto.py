#!/usr/bin/python
# -*- coding: UTF-8 -*-
import scrapy
import pymysql
import logging
import time
import json
import requests
import random
import re


class mingyan(scrapy.Spider): 
    
    name = "article_auto"
    start_urls = [
        'http://health.people.com.cn',
        'http://health.people.com.cn/GB/405407/index.html',
        'http://health.people.com.cn/GB/408572/index.html',
    ]

    def parse(self, response):

        # 人民网 综合新闻
        if response.url == 'http://health.people.com.cn':
            
            urls = self.health_people_journalism(response)
            for val in urls:
                # search拼接
                img_url = self.img_url(val)
                meta_data = {'img_path': urls[val][1]+urls[val][2],'acticle_title': val,'image':urls[val][2],'url':urls[val][0],'callback':self.health_people_journalism}
                # baidu img API
                yield scrapy.Request(url=img_url, meta=meta_data, callback=self.img_handle,dont_filter=True)
            
        # 中药
        elif response.url == 'http://health.people.com.cn/GB/405407/index.html':
            urls = self.health_people_technology(response)
            for val in urls:
                # search拼接
                img_url = self.img_url(val)
                meta_data = {'img_path': urls[val][1]+urls[val][2],'acticle_title': val,'image':urls[val][2],'url':urls[val][0],'callback':self.health_people_technology}
                # baidu img API
                yield scrapy.Request(url=img_url, meta=meta_data, callback=self.img_handle,dont_filter=True)

        # 养生
        elif response.url == 'http://health.people.com.cn/GB/408572/index.html':
            urls = self.health_people_health(response)
            for val in urls:
                # search拼接
                img_url = self.img_url(val)
                meta_data = {'img_path': urls[val][1]+urls[val][2],'acticle_title': val,'image':urls[val][2],'url':urls[val][0],'callback':self.health_people_health}
                # baidu img API
                yield scrapy.Request(url=img_url, meta=meta_data, callback=self.img_handle,dont_filter=True)


    def health_people_journalism(self,response):
        
        db = pymysql.connect("localhost", "db_username", "db_pssword", "db_database", charset='utf8')

        # 使用cursor()方法获取操作游标 
        cursor = db.cursor()
        title = {}

        if response.meta['depth'] == 0:
            # 文章标题
            titles = [response.css('.topicNews h1 a::text').extract()[0]]
            # img路径
            title[titles[0]] = [response.url+response.css('.topicNews h1 a::attr(href)').extract()[1],'news/','n'+str(int(time.time()))+str(random.randint(0,1000))+'.jpg']
        
            sql = "SELECT title FROM he_news WHERE title = %s" # or title = %s
            # 使用execute方法执行SQL语句
            cursor.execute(sql,[titles[0]])
            res = cursor.fetchall()
            
            # 库中已有此文章就不再爬了
            for val in res:
                del title[val[0]]

        else:
            
            # 获取正文及来源
            health_people_acticle = self.health_people_acticle(response)
            
            # 正则去除不需要的东西
            content = re.compile('<a href(.*?)</a>').sub('', health_people_acticle[1])
            # content = re.compile('<img(.*?)>').sub('', content)
            content = re.compile('width="(.*?)"').sub('', content)

            # 文章添加图片
            content = '<img src="/Uploads/news/'+response.meta['image']+'">'+content;
            
            # 拼sql
            cursor.execute('SELECT MAX(nid) FROM he_news')
            priority = cursor.fetchone()[0] + 1
            sql = "INSERT INTO he_news (title,aid,addtime,image,info,type,priority,source) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
            vals = (response.meta['acticle_title'],9,int(time.time()),response.meta['image'],content,11,priority,health_people_acticle[0])
            
            try:            
                # 图片写入
                imgs_url = requests.get(response.meta['imgs_url'])
                with open("/server/Uploads/"+response.meta['img_path'], 'wb')as jpg:
                    jpg.write(imgs_url.content)
                # 执行sql
                cursor.execute(sql,vals)

            except Exception as e:
                logging.error('添加文章失败')
                logging.error(e)
            else:
                logging.info('成功添加文章')
                logging.info(response.meta['acticle_title'])

            db.commit()
        
        # 关闭数据库连接
        db.close()  
        return title


    def health_people_technology(self,response):
        db = pymysql.connect("localhost", "db_username", "db_pssword", "db_database", charset='utf8')

        # 使用cursor()方法获取操作游标
        cursor = db.cursor()
        title = {}

        if response.meta['depth'] == 0:
      
            acticle_title = []
            result = response.xpath("//ul[@class='list_02']/div[@class='newsItems']/a")
            for val in result:
                acticle_title.append(val.xpath("string(.)").extract_first())
                title[val.xpath("string(.)").extract_first()] = ['http://health.people.com.cn' + val.xpath("@href").extract_first(),'team/','t'+str(int(time.time()))+str(random.randint(0,1000))+'.jpg']

            del result
            sql = "SELECT title FROM he_team WHERE title = %s or title = %s or title = %s or title = %s or title = %s"

            # 使用execute方法执行SQL语句
            cursor.execute(sql, acticle_title)
            del sql
            del acticle_title
            res = cursor.fetchall()

            for val in res:
                del title[val[0]]
            del res

        else:
            # 获取正文及来源
            health_people_acticle = self.health_people_acticle(response)

            # 正则去除不需要的东西
            content = re.compile('<a href(.*?)</a>').sub('', health_people_acticle[1])
            # content = re.compile('<img(.*?)>').sub('', content)
            content = re.compile('width="(.*?)"').sub('', content)
            
            # 文章添加图片
            content = '<img src="/Uploads/team/'+response.meta['image']+'">'+content;
            
            # 拼sql
            cursor.execute('SELECT MAX(tid) FROM he_team')
            priority = cursor.fetchone()[0] +1
            sql = "INSERT INTO he_team (title,aid,addtime,image,info,priority,cid,source) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
            vals = (response.meta['acticle_title'], 9, int(time.time()), response.meta['image'], content, priority,  41, health_people_acticle[0])
            
            try:            
                # 图片写入
                imgs_url = requests.get(response.meta['imgs_url'])
                with open("/server/Uploads/"+response.meta['img_path'], 'wb')as jpg:
                    jpg.write(imgs_url.content)
                # 执行sql
                cursor.execute(sql,vals)

            except Exception as e:
                logging.error('添加文章失败')
                logging.error(e)
            else:
                logging.info('成功添加文章')
                logging.info(response.meta['acticle_title'])

            db.commit()

        # 关闭数据库连接
        db.close()
        return title


    def health_people_health(self,response):
        db = pymysql.connect("localhost", "db_username", "db_pssword", "db_database", charset='utf8')

        # 使用cursor()方法获取操作游标
        cursor = db.cursor()
        title = {}

        if response.meta['depth'] == 0:

            acticle_title = []
            result = response.xpath("//ul[@class='list_02']/div[@class='newsItems']/a")
            for val in result:
                acticle_title.append(val.xpath("string(.)").extract_first())
                title[val.xpath("string(.)").extract_first()] = ['http://health.people.com.cn' + val.xpath("@href").extract_first(),'team/','t'+str(int(time.time()))+str(random.randint(0,1000))+'.jpg']

            del result
            sql = "SELECT title FROM he_team WHERE title = %s or title = %s or title = %s or title = %s or title = %s"

            # 使用execute方法执行SQL语句
            cursor.execute(sql, acticle_title)
            del sql
            del acticle_title
            res = cursor.fetchall()

            for val in res:
                del title[val[0]]
            del res
        else:
            # 获取正文及来源
            health_people_acticle = self.health_people_acticle(response)

            # 正则去除不需要的东西
            content = re.compile('<a href(.*?)</a>').sub('', health_people_acticle[1])
            # content = re.compile('<img(.*?)>').sub('', content)
            content = re.compile('width="(.*?)"').sub('', content)

            # 文章添加图片
            content = '<img src="/Uploads/team/'+response.meta['image']+'">'+content;
            # 拼sql
            cursor.execute('SELECT MAX(tid) FROM he_team')
            priority = cursor.fetchone()[0] +1
            sql = "INSERT INTO he_team (title,aid,addtime,image,info,priority,cid,source) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
            vals = (response.meta['acticle_title'], 9, int(time.time()), response.meta['image'], content, priority,6, health_people_acticle[0])
            
            try:            
                # 图片写入
                imgs_url = requests.get(response.meta['imgs_url'])
                with open("/server/Uploads/"+response.meta['img_path'], 'wb')as jpg:
                    jpg.write(imgs_url.content)
                # 执行sql
                cursor.execute(sql,vals)
                
            except Exception as e:
                logging.error('添加文章失败')
                logging.error(e)
            else:
                logging.info('成功添加文章')
                logging.info(response.meta['acticle_title'])

            db.commit()

        # 关闭数据库连接
        db.close()

        return title


    def img_handle(self,response):
        
        # 进入存储照片
        imgs_url = json.loads(response.body.decode('utf-8'))
        
        if 'data' in imgs_url:
            # 成功获取图片url
            imgs_url = requests.get(imgs_url['data'][0]['middleURL'])

            meta_data = {'acticle_title': response.meta['acticle_title'],'image':response.meta['image'],'img_path':response.meta['img_path'],'imgs_url':imgs_url}
            # 进行最后的入库即报错图片
            yield scrapy.Request(url=response.meta['url'],meta=meta_data,callback=response.meta['callback'],dont_filter=True)
            
        else:
            # 被拉入黑名单接口获取失败
            logging.warning('文章“'+response.meta['acticle_title']+'”图片获取失败')

    def img_url(self,search):
        return 'https://image.baidu.com/search/acjson?tn=resultjson_com&ipn=rj&ct=201326592&is=&fp=result&queryWord='+search+'&cl=2&lm=-1&ie=utf-8&oe=utf-8&adpicid=&st=-1&z=&ic=0&word='+search+'&s=&se=&tab=&width=&height=&face=0&istype=2&qc=&nc=1&fr=&pn=0&&rn=1'

    def health_people_acticle(self,response):
        # 来源
        source = response.xpath("//div[@class='artOri']//a/text()").extract()[0]
        # 正文
        info = ''.join(response.xpath("//div[@class='artDet']").extract()[0])
        # info += '<p>(来源：' + source + ')</p>
        return (source,info)


