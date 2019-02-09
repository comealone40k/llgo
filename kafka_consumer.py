from kafka import KafkaConsumer
import json
import time

# To consume latest messages and auto-commit offsets
consumer = KafkaConsumer('liligo',
                         group_id='my-group',
                         bootstrap_servers=['localhost:9092'],
						 consumer_timeout_ms=100)

## Loop consumed messages
while True:
	eventlist = []
	for message in consumer:
		eventlist.append(tuple((json.loads(message.value)['type'],json.loads(message.value)['count'])))
		#print(json.loads(message.value)['count'])
		#clickcount = clickcount + json.loads(message.value)['count']
		#print("Inner: "+str(clickcount))
		
	templist = {}
	
	for item in eventlist:
		templist[item[0]]=0	
	
	for item in eventlist:
		templist[item[0]]=templist[item[0]] + item[1]	
	
	print(time.strftime("%Y-%m-%d %H:%M:%S"))
	for item in templist:
		print("Count for " + item + " is " + str(templist[item]))	
	
	time.sleep(10)
