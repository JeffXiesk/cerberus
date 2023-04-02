import sqlite3
import os

for dir in os.listdir('data'):
    # print(dir)
    
    con = sqlite3.connect("data/"+str(dir))
    c = con.cursor()

    cursor = c.execute(
        '''
        SELECT 10 * 1000000.0 * count() / (max(ts) - min(ts))
        FROM request_truncated
        WHERE event = 'REQ_FINISHED';
        '''
    )

    print('----------'+str(dir)+'----------')
    for row in cursor:
       print("throughput = ",row[0])

    cursor = c.execute(
        '''
        SELECT avg(latency) / 1000.0
        FROM request_truncated
        WHERE event = 'REQ_FINISHED';
        '''
    )

    for row in cursor:
       print("latency = ",row[0])

    con.close()
    

print ("数据库打开成功")