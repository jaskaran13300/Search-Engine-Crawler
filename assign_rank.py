import sqlite3
conn = sqlite3.connect('crawler.sqlite')
cur = conn.cursor()

from_ids=list()

try:
    cur.execute('select distinct from_id from links')
except:
    print('Firstly Crawl the pages by running crawler.py')
    quit()
for row in cur:
    from_ids.append(row[0])

# print(from_ids)

to_ids=list()
links=list()

cur.execute('select distinct from_id,to_id from links')
for row in cur:
    # print(row)
    from_id=row[0]
    to_id=row[1]
    if from_id==to_id:
        continue
    if from_id not in from_ids:
        continue
    if to_id not in from_ids:
        continue
    links.append(row)
    if to_id not in to_ids:
        to_ids.append(to_id)

# print(links)

prev_ranks=dict()

for id in from_ids:
    cur.execute('select new_rank from pages where id=?',(id, ))
    row=cur.fetchone()
    prev_ranks[id]=row[0]

# print(prev_ranks)

sval=input('Enter Number of iterations - ')
if len(sval)<=0:
    sval='1'
ival=int(sval)

if len(prev_ranks)<=0:
    print('Nothing to page rank. Check data')
    quit()
print(prev_ranks)
print(links)
for i in range(ival):
    new_ranks=dict()
    for (node,old_rank) in list(prev_ranks.items()):
        give_ids=list()
        for (from_id,to_id) in links:
            
            if node !=to_id:
                continue
            if from_id in give_ids:
                continue
            give_ids.append(from_id)
        l=len(give_ids)
        if l<=0:
            continue
        total=0.0
        l=0
        print('give ids are ',give_ids)
        # give ids contains a,c for B
        for idd in give_ids:
            temp=list()
            for (from_id,to_id) in links:
                if from_id==idd:
                    if to_id in temp:
                        continue
                    else:
                        temp.append(to_id)
            total=total+(prev_ranks[idd]/len(temp))
            # print(len(temp))
        new_ranks[node]=total
        print(new_ranks)    

