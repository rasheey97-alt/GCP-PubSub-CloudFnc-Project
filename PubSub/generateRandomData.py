import argparse
import os
import sys
import random
from scipy import stats
import datetime
import time
from google.cloud import pubsub_v1

def run(TOPIC_NAME, PROJECT_ID, INTERVAL = 200):

	publisher = pubsub_v1.PublisherClient()

	topic_path = publisher.topic_path(PROJECT_ID,TOPIC_NAME)

	sensorNames = ['Pressure_1','Pressure_2','Pressure_3','Pressure_4','Pressure_5']
	sensorCenterLines = [1992,2080,2390,1732,1911]
	standardDeviation = [442,388,354,403,366]

	c=0

	while(True):
		for pos in range(0,5):		
			sensor = sensorNames[pos];
			reading = stats.truncnorm.rvs(-1,1,loc = sensorCenterLines[pos], scale = standardDeviation[pos])
			timeStamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
			message = timeStamp+','+sensor+','+str(reading)
			publisher.publish(topic_path, data=message.encode('utf-8'))
			c=c+1
		time.sleep(INTERVAL/1000)
		if c == 100:
			print("Published 100 Messages")
			c=0


if __name__ == "__main__":  # noqa
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--TOPIC_NAME",
        help="The Cloud Pub/Sub topic to write to.\n"
        '"<TOPIC_NAME>".',
    )
    parser.add_argument(
        "--PROJECT_ID",
        help="GCP Project ID.\n"
        '"<PROJECT_ID>".',
    )
    parser.add_argument(
        "--INTERVAL",
        type=int,
        default=200,
        help="Interval in mili seconds which will publish messages (default 2 ms).\n"
        '"<INTERVAL>"',
    )
    args = parser.parse_args()
    try:
        run(
        args.TOPIC_NAME,
        args.PROJECT_ID,
        args.INTERVAL
    	)
    except KeyboardInterrupt:
        print('Interrupted : Stopped Publishing messages')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
    