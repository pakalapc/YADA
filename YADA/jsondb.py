#
# Script Name :jsondb.py
# Description : To parse the json file 
#		and convert to flat file
#
# Author : Chandni Pakalapati
#	 : Praveen Iyengar
#	 : Priyanka Samanta


import csv
import json
from builtins import map



b_file=open('businesspkey.csv','r')
bkeystring=b_file.readline()


u_file=open('userpkey.csv','r')
ukeystring=u_file.readline()

blist=bkeystring.split(',')
ulist=ukeystring.split(',')



user_file=open("yelp_user.json",'r')
user=open('user.csv', 'w', newline='')

q=open("yelp_1.json",'r')
f=open('business.csv', 'w', newline='')
spamwriter = csv.writer(f)

k=1
while(k<11):
    k=k+1
    filename= str(k) +"-review"
    print(filename)
    review_file=open(filename+".json",'r')
    freview=open(filename+".csv",'w',newline='')
    reviewwriter=csv.writer(freview)
    
   
    f_categories=open('category.csv', 'w', newline='')
    f_neighborhood=open('neighborhood.csv', 'w', newline='')
    write_categories=csv.writer(f_categories)
    write_neighborhood=csv.writer(f_neighborhood)
   
    count=0
    count_1=0
    for line in review_file:
        try:
            y= json.loads(line)
        except ValueError as e:
            continue
        if y['user_id'] in ulist and y['business_id'] in blist:
            
            count=count+1 
            rrow=[]
            rrow.append(y['review_id'])
            rrow.append(y['user_id'])
            rrow.append(y['business_id'])
            rrow.append(y['stars'])
            reviewwriter.writerow(rrow)
            if(count>1000):
                print(count)
                count =0
        else:
            count_1=count_1+1
            if(count_1>10000):
                print("count_1" + str(count_1))
                count_1=0
    freview.close()
for line in q:
    
    try:
        y= json.loads(line)
    except ValueError as e:
        continue
    row=[]
    row.append(y['business_id'])
   
    if y['open']:
        row.append("Yes")
    else:
        row.append("No")
    row.append(y['full_address'])
    row.append(y['city'])
    row.append(y['state'])
    row.append(y['latitude'])
    row.append(y['longitude'])
    row.append(y['stars'])
    row.append(y['name'])
    row_category=[]
    try:
        if y['categories'][0] is not None:
            row.append(y['categories'][0])
    except IndexError as e:
        continue 
    row_category.append(y['categories'][0])
    write_categories.writerow(row_category)
        #add every 'cell' to the row list, identifying the item just like an index in a list
    row_neigh=[]
    try:
        if y['neighborhoods'][0] is not None:
            row_neigh.append(y['neighborhoods'][0])
            row_neigh.append(y['city'])
            row_neigh.append(y['state'])
            row.append(y['neighborhoods'][0])
            row.append(y['city'])
            row.append(y['state'])

    except IndexError as e:
        continue
     
    write_neighborhood.writerow(row_neigh)
    

    spamwriter.writerow(row)
       # row.append(str(pk["created_at"].encode('utf-8')))
        #row.append(str(pk["text"].encode('utf-8'))
    spamwriter_user = csv.writer(user)

    for line_user in user_file:
        
        try:
            user_j= json.loads(line_user)
        except ValueError as e:
            continue
        row=[]
        row.append(user_j['user_id'])
        row.append(user_j['average_stars'])
        row.append(user_j['name'])
        row.append(user_j['review_count'])
        try:
            dict_votes=[]
            dict_votes =user_j['votes']
            row.append(dict_votes['useful'])
        except KeyError as e:
            continue
        
        try:
            spamwriter_user.writerow(row)
        except UnicodeEncodeError :
            continue



