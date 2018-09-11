#！ --*--coding:utf-8--*--
import linecache
import time
import re

now = time.time()
data_keys = (('bid','uid','username','v_class','content','img','created_at','source',
             'rt_num','cm_num','rt_uid','rt_username','rt_v_class','rt_content','rt_img',
             'src_rt_num','src_cm_num','gender','rt_bid','location','rt_mid','mid','lat',
             'lon','lbs_type','lbs_title','poiid','links','hashtags','ats','rt_hashtags',
             'rt_ats','v_url','rt_v_url'))
keys = {data_keys[k]:k for k in range(0,len(data_keys))}
print(keys)
f = linecache.getlines('twitter数据挖掘片段.txt')[0:100]
lines = [x[1:-1].split('","') for x in f]

users = set(line[keys['username']] for line in lines)
user_total = len(users)
assert type(user_total)==int
print('1.用户个数：',user_total)

users = list(users)
assert type(users)==list
print("2.用户名：",users)

lines_total_2012_11 = 0
for line in lines:
    if line[keys['created_at']].startswith('2012-11'):
        lines_total_2012_11 += 1
assert type(lines_total_2012_11)==int
print("3.2012年11月发布的tweets数：",lines_total_2012_11)

users_by_data = [line[keys['created_at']].split(' ')[0] for line in lines]
lines_by_created = list(set(users_by_data))
lines_by_created.sort()
assert type(lines_by_created)==list
print("4.发布微博的日期有：",lines_by_created)

hours = [int(line[keys['created_at']][11:13]) for line in lines]
total_by_hour = [(h,hours.count(h)) for h in hours]
total_by_hour.sort(key = lambda k:k[1],reverse=True)
max_hour = total_by_hour[0][0]
assert type(max_hour)==int
print("5.一天中哪个小时发布tweets最多：",max_hour)

dateline_by_user = { k:dict() for k in lines_by_created }
#print(dateline_by_user)
for line in lines:
    dateline = line[keys['created_at']].split(' ')[0]
    username = line[keys['username']]
    if dateline in dateline_by_user.keys():
        dateline_by_user[dateline][username] =0
        #print(dateline_by_user[dateline][username])
        #先确定日期中有哪些人发tweets了，即确定子key
for line in lines:
    dateline = line[keys['created_at']].split(' ')[0]
    username = line[keys['username']]
    if dateline in dateline_by_user.keys():
        dateline_by_user[dateline][username] +=1
        #print(dateline_by_user.items())
        #再根据发微博次数，统计频率
for k,v in dateline_by_user.items():
    us = list(v.items())
    us.sort(key = lambda k:k[1],reverse=True)
    dateline_by_user[k] = { us[0][0]:us[0][1] }
assert type(dateline_by_user) == dict
print("6.每一天发tweets最多的用户：",dateline_by_user)


lines_from_2012_11_03 = filter(lambda line:line[keys['created_at']].startswith('2012-11-03'),lines)
hourlines_from_2012_11_03 = {str(i):0 for i in range(0,24)}
for line in lines_from_2012_11_03:
    hour = line[keys['created_at']][11:13]
    hourlines_from_2012_11_03[str(int(hour))] += 1

hourlines_from_2012_11_03 = [(k,v) for k,v in hourlines_from_2012_11_03.items()]
hourlines_from_2012_11_03.sort(key=lambda k:int(k[0]))
assert type(hourlines_from_2012_11_03)==list
print("7.2012年11月03日各时段发布的微博数：",hourlines_from_2012_11_03)

source = set([line[keys['source']] for line in lines])
source_dict = { s:0 for s in source }
for line in lines:
    source_name  = line[keys['source']]
    source_dict[source_name] += 1
source_list = [(k,v) for k,v in source_dict.items()]
source_list.sort(key=lambda k:k[1],reverse=True)
assert type(source_list)==list
print("8.发微博工具来源：",source_list)

umi_total = 0
for line in lines:
    if line[keys['rt_v_url']].startswith('https://twitter.com/umiushi_no_uta'):
        umi_total += 1
assert type(umi_total)==int
print("9.转发中以某url开头的微博数：",umi_total)

tweets_total_from_573638104 = 0
for line in lines:
    if line[keys['uid']] == '573638104':
        tweets_total_from_573638104 += 1
assert type(tweets_total_from_573638104)==int
print("10.某UID用户发微博总数：",tweets_total_from_573638104)

def get_usr_by_max_tweets(*uids):
    return "Null"

lines_by_content_length = [(line[keys['username']],len(line[keys['content']])) for line in lines]
lines_by_content_length.sort(key=lambda k:k[1],reverse=True)
users_by_max_content = lines_by_content_length[0][0]
assert  type(users_by_max_content)==str
print("12.发单条微博最长的用户为：",users_by_max_content)

lines_by_rt = [(line[keys['uid']],int(line[keys['rt_num']])) for line in lines if line[keys['rt_num']] != '']
lines_by_rt.sort(key = lambda k:k[1],reverse=True)
users_by_max_rt = lines_by_rt[0][0]
assert type(users_by_max_rt)==str
print("13.谁转发的url最多：",users_by_max_rt)

lines_on_hour11 = filter(lambda line:line[keys['created_at']].startswith('11',11,13),lines)
lines_by_uid_on_hour11 = { k[keys['uid']]:0 for k in lines_on_hour11 }
for line in lines_on_hour11:
    uid = line[keys['uid']]
    lines_by_uid_on_hour11[uid] += 1
d = [(k,v) for k,v in lines_by_uid_on_hour11.items()]
d.sort(key=lambda k:k[1])
uid_by_max_tweets_on_hour11 = d[0][0]
assert type(uid_by_max_tweets_on_hour11)==str
print("14.谁在11点发的微博次数最多：",uid_by_max_tweets_on_hour11)

uid_by_v_url = {k[keys['uid']]:0 for k in lines}
for line in lines:
    uid = line[keys['uid']]
    if line[keys['v_url']] != '':
        uid_by_v_url[uid] += 1
uid_sort_by_b_url = [(k,v) for k,v in uid_by_v_url.items()]
uid_sort_by_b_url.sort(key=lambda k:k[1],reverse=True)
uid_by_max_v_url = uid_sort_by_b_url[0][0]
assert type(uid_by_max_v_url)==str
print("15.哪个用户发源微博url次数最多：",uid_by_max_v_url)

print("运算时间：%s" %(time.time() - now))