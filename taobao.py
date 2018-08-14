# -*- coding: UTF-8 -*-
from selenium import webdriver
import selenium
import time
import requests
# from selenium.webdriver import ActionChains#模拟动作
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from multiprocessing import Pool
import re
import os
import  csv
import random
from lxml import etree
from bs4 import BeautifulSoup


def ip_proxy():
    '''
    从代理IP接口获取一批代理IP，个数取决于代理IP的设置
    :return:
    '''
    url = 'http://api.ip.data5u.com/dynamic/get.html?order=02c36df29a90e4f8e28f8c6e39dc59d9&sep=0'
    data = requests.get(url)
    proxy = data.text
    ips = proxy.split('\r\n')

    # ips = []
    # for ip in ips:
    #     if ip != '':
    #         ips.append(ip)

    return ips[:-1]

def get_drive(load_time,ip = ''):
    '''
    获得一个限制get load时长的浏览器
    :param load_time:
    :param ip:
    :return:
    '''
    if ip == '':
        print('IP:'+'本机')
    else:
        print('IP:' + ip)

    option = webdriver.ChromeOptions()
    # option.add_argument('--headless')  #隐藏浏览器，有时被检测到导致爬取失败
    prefs = {
        'profile.default_content_setting_values': {
            'images': 2,
            # 'javascript': 2
        }}
    option.add_experimental_option("prefs", prefs)

    option.add_argument('--proxy-server=' + ip)
    driver = webdriver.Chrome(chrome_options = option)
    driver.set_page_load_timeout(load_time)  #最长加载时间，超过会抛出异常
    return driver


def get_shop_urls(store_url: object) -> object:
    '''
    获取排序页宝贝的链接和销量
    :param store_url:
    :return:
    '''
    driver = get_drive(40)
    while True:
        try:
            driver.get(store_url)
            soup = BeautifulSoup(driver.page_source, 'lxml')
            see_code = print(soup.prettify())
            page_all = soup.select('div.pagination.pagination-mini > span')[0].text
            page_all = int(page_all.split('/')[1])
            print('店铺源码爬取成功，一共{}页'.format(page_all))
            break
        except TimeoutException:
            driver.close()
            ip = random.choice(ip_proxy())
            driver = get_drive(40,ip)
        except IndexError:
            driver.close()
            time.sleep(2) #爬取到错误的源码需要等待
            ip = random.choice(ip_proxy())
            driver = get_drive(40,ip)
    #主体
    page_shop_urls = []
    for i in range(1,page_all+1):

        #获取宝贝链接，销量对应的tag  List
        soup = BeautifulSoup(driver.page_source, 'lxml')
        urls = soup.select('#J_ShopSearchResult > div > div.shop-hesper-bd.grid > div > dl > dt > a')
        xls = soup.select('#J_ShopSearchResult > div > div.shop-hesper-bd.grid > div > dl > dd.detail > div > div.sale-area > span')
        print('第{}页：{}个宝贝,{}个销量'.format(i,len(urls),len(xls)),end='')

        #获取tag中的信息
        for url,xl in zip(urls,xls):
            page_shop_urls.append([url.get('href'),xl.get_text()])

        #获取下一页
        selector = etree.HTML(driver.page_source.encode('utf-8'))
        try:
            next_page_url = selector.xpath('//*[@id="J_ShopSearchResult"]/div/div[2]/div[10]/a[11]/@href')[0]
            print('--->成功翻至下一页')
        except:
            if i == page_all:
                print('--->已经翻至最后一页')
                continue
            print('获取下一页失败，刷新')
            driver.refresh()

        #判断是否需要切换
        while True:
            try:
                print('pageok waiteNs get...',end='')
                time.sleep(5)
                # time.sleep(random.choice([2,3,4,5,6]))
                driver.get('https:' + next_page_url)
                print('get ok...',end='')
                soup = BeautifulSoup(driver.page_source, 'lxml')
                nest_page_test = soup.select('div.pagination.pagination-mini > a')[0].text
                print('check ok...break')
                break
            except Exception as e:#异常的父类
                try:
                    if isinstance(e,IndexError):
                        print('check ng...refresh...',end='')
                    else :
                        print('get ng...timeout...refresh...', end='')
                    driver.refresh()
                    soup = BeautifulSoup(driver.page_source, 'lxml')
                    nest_page_test = soup.select('div.pagination.pagination-mini > a')[0].text
                    print('check ok...break')
                    break
                except:
                    print('check ng...close')
                    # time.sleep()
                    driver.close()
                    ip = random.choice(ip_proxy())
                    driver = get_drive(30, ip)

    return  page_shop_urls


def multicore_ctrl(pro_num, urls_all_store):
    '''
    充分利用代理IP的前提下对多进程进行调度
    :param pro_num:
    :param urls_all_store:
    :return:
    '''
    # ips_useful = ip_proxy()
    pool = Pool(pro_num)
    f = open('data_tb.csv', 'a+')
    csv_headers = ['标题', '销量', '价格', '收藏', '店铺', '宝贝链接','图片索引']
    f_csv = csv.DictWriter(f, csv_headers)
    f_csv.writeheader()
    ips_usf = []
    count = 0
    now = 0
    while True:
        count += 1
        ips_ucnt = len(ips_usf)
        if ips_ucnt == 0:
            # ips_usf = ip_proxy()#在这里导入代理IP
            ips_usf = ['','','','','','','','','','']#本机

            end = now  #计时器
            now = time.time()
            if count == 1:
                time_ip = 0
            else :
                time_ip = now - end
            ips_ucnt = len(ips_usf)
            print('第{}次提取IP时间间隔：{:.4f}，IP数：{}'.format(count, time_ip,ips_ucnt))

        urls = urls_all_store[0:ips_ucnt]
        urls_all_store = urls_all_store[ips_ucnt:]#留着后面用
        urls_ip = list(zip(urls, ips_usf))

        #使用多进程，最好进程数==ips_ucnt
        result = pool.map(get_info,urls_ip)#pool map是阻塞式的，即得到result后才往下执行

        # #不使用多进程，调试用
        # result = get_info(urls_ip[0])
        # result = [result]

        #解析result
        ips_usf = []
        baby_inf = []
        for pro_res in result:
            if pro_res['ip_state'] == 'ok':
                ips_usf.append(pro_res['ip'])#回收ip
                baby_inf.append(pro_res['baby_inf'])
                f_csv.writerows(baby_inf)#储存一个宝贝
            else:
                urls_all_store.append(pro_res['url'])

        urls_nopro_num = len(urls_all_store)
        if urls_nopro_num == 0:
            break
        else:
            print('未处理的urls:{}'.format(urls_nopro_num))
    pool.close()
    f.close()


def get_save_img(img_urls, titles, path):
    '''
    保存宝贝的图片
    :param img_urls:
    :param titles:
    :param path:
    :return:
    '''
    count = 0
    img_file_dirs = []
    for img_url in img_urls:
        count += 1
        img_url = 'http:%s'%img_url
        try:
            img = requests.get(img_url)
        except:
            continue
        if os.path.exists(path) == False:
            os.makedirs(path)
        img_file_dir = path+'/%s%s.png'%(titles,count)
        with open(img_file_dir,'wb') as f:
            f.write(img.content)
            img_file_dirs.append(img_file_dir)

    return img_file_dirs


def get_info(urls_ip):
    '''
    获取宝贝的详细信息
    :param urls_ip:
    :return:
    '''
    urls = urls_ip[0]
    url = urls[0]
    xl = urls[1]
    ip = urls_ip[1]

    try:
        driver = get_drive(20, ip=ip)
        t1 = time.time()
        driver.get('https:%s'%url)
        html = driver.page_source
        print('get source',end='')
    except TimeoutException:
        t2 = time.time()#用这句可以验证确实是10s后抛出异常
        print('time out:{:.4f}'.format(t2-t1))
        driver.execute_script('window.stop()')#网传这句有用其实没用
        # html = driver.page_source
        #基于以下分析目前技术解决不了，出现timeout 只能先关闭drive了
        driver.close()
        return {'ip_state': 'ng', 'urls': urls, 'ip': ip}
    '''在实例化driver的时候，隐式等待貌似没啥作用，get还是一直卡着，也就不能往后去执行显示等待了（感觉显示等待就是用在sent一个东西等网页回复的这种情况）'''
    '''在实例化driver的时候，限制load的时间到时有作用，可以使得driver在load时间之内还没有完全load则抛出timeout的异常'''
    '''现在的问题是一旦get抛出超时异常，driver.page_source就执行不了，相当于整个driver对象就废了，但是我们从网页上看想要的信息都已经加载了'''

    #能到这里说明不是timeout这种情况。先检测有没有价格,没有就关闭
    try:
        et = etree.HTML(driver.page_source)
        # jg = et.xpath('//*[@id="J_PromoPriceNum"]/text()')[0]#价格是最重要指标，用价格做检查
        jg = et.xpath('//*[@id="J_SellCounter"]/text()')[0]  # 销量，经试验有这个Tag但是得不到数值，只得到-
        # jg = et.xpath('//*[@class="tb-rmb-num"]/text()')[0] # 用这个class更好，id要很久才能加载出来，以至于找不到这个Tag抛出异常

        # driver.find_element_by_xpath('//*[@id="J_PromoPriceNum"]')
        # driver.find_element_by_xpath('//div[@class="tb-wrap tb-wrap-newshop"]')
        # driver.find_element_by_id("J_PromoPriceNum")
        print('..have jg...',end='')
    except :
        print('..no   jg...',end='')
        driver.close()
        return {'ip_state': 'ng', 'urls': urls, 'ip': ip}

    #baby titles
    try:
        et = etree.HTML(driver.page_source)
        titles = et.xpath('//*[@id="J_Title"]/h3/@data-title')[0]
        titles = re.sub('[\/:*?"<>|]', '-', titles)
    except IndexError:
        titles = None

    # baby images
    img_urls = et.xpath('//*[@id="J_UlThumb"]/li/div/a/img/@data-src')
    img_save_dirs = get_save_img(img_urls, titles, './imgs_tb')

    #收藏
    try:
        sc = re.findall('.*?(\d+).*?',str(et.xpath('//*[@id="J_Social"]/ul/li[1]/a/em/text()')))[0]
    except IndexError:
        sc = None

    #店铺名
    try:
        dp = str(et.xpath('//*[@id="J_ShopInfo"]/div/div[1]/div[1]/dl/dd/strong/a/text()')[0]).strip()
    except IndexError:
        dp = None

    print(titles,xl,jg,sc,dp,url,img_save_dirs)
    baby_inf = [titles,xl,jg,sc,dp,url,img_save_dirs]
    csv_headers = ['标题', '销量', '价格', '收藏', '店铺', '宝贝链接', '图片索引']
    baby_inf_dict = dict(zip(csv_headers,baby_inf))#组合成字典才能存进csv
    driver.close()
    return  {'ip_state':'ok','url':url,'ip':ip,'baby_inf':baby_inf_dict}


def save_urls(name, urls):
    '''
    保存从排序页面爬到的url和价格
    :param name:
    :param urls:
    :return:
    '''
    with open(name,'w') as f:
        for url in urls:
            url = ','.join(url)
            f.write(url+'\n')

def read_urls(name):
    '''
    读取从排序页面爬到的url和价格
    :param name:
    :return:
    '''
    with open(name,'r') as f:
        urls = []
        urls_temp = f.readlines()
        for url in urls_temp:
            urls.append(url.strip('\n').split(','))
    return urls


def read_store(name):
    '''
    读取保存的店铺主页链接
    :param name:
    :return:
    '''
    with open(name,'r') as f:
        stores = []
        stores_temp = f.readlines()
        for store in stores_temp:
            stores.append(store.strip('\n'))
    return stores


def multicore(pro_num, urls_all_store):
    '''
    不使用代理IP，不调度多进程，简单粗暴地开启多进程
    :param pro_num:
    :param urls_all_store:
    :return:
    '''
    pool = Pool(pro_num)
    f = open('data_tb.csv', 'a+')
    csv_headers = ['标题', '销量', '价格', '收藏', '店铺', '宝贝链接','图片索引']
    f_csv = csv.DictWriter(f, csv_headers)
    f_csv.writeheader()
    ng_cnt_nest = 0

    while True:
        ips_usf = len(urls_all_store) * ['']
        urls_ip = list(zip(urls_all_store, ips_usf))
        results = pool.map(get_info, urls_ip)  # pool map是阻塞式的，即得到result后才往下执行

        urls_all_store = []
        baby_inf = []
        for pro_res in results:
            if pro_res['ip_state'] == 'ok':
                baby_inf.append(pro_res['baby_inf'])
                f_csv.writerows(baby_inf)  # 储存一个宝贝
            else:
                urls_all_store.append(pro_res['urls'])
        ng_cnt = len(urls_all_store)

        if ng_cnt == 0:
            print('宝贝详情采集完成')
            break
        else:
            print('next batch:{}'.format(ng_cnt))
            if ng_cnt_nest == ng_cnt:
                print('宝贝详情采集完成(部分商品下架或过期)')
                break
            else:
                ng_cnt_nest = ng_cnt

    f.close()


if __name__ == '__main__':
    '''
        主函数入口
    '''

    '''step1: 获取所有宝贝的URL（和销量）'''
    stores = read_store('stores.txt')
    urls_all_store = []
    for store_url in stores:
        urls_one_store = get_shop_urls(store_url)
        urls_all_store = urls_all_store + urls_one_store

    print('成功爬取{}个店铺共{}个宝贝的链接和销量,正在备份...'.format(len(stores),len(urls_all_store)))
    save_urls('baby_urls.txt', urls_all_store)#备份
    print('备份完成！！')

    '''step2: 多线程获取宝贝的详细信息'''
    pro_num = 8
    urls_all_store = read_urls('baby_urls.txt')

    # print('启用代理，调度{}进程爬取宝贝详细信息...'.format(pro_num))
    # multicore_ctrl(pro_num, urls_all_store)  #开启多进程调度

    print('启动{}个进程爬取宝贝详细信息...'.format(pro_num))
    s = time.time()
    multicore(pro_num, urls_all_store)
    t = time.time()
    print('总用时：{:.4f}秒'.format(t-s))
