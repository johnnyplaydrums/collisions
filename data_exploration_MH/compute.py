# Spark submit command
#spark-submit --name "bike_taxi" --files hdfs:///user/jspence01/collisions/march.csv,hdfs:///user/jspence01/collisions/april.csv,hdfs:///user/jspence01/collisions/may.csv,hdfs:///user/jspence01/collisions/june.csv,hdfs:///user/jspence01/collisions/february.csv,hdfs:///user/jspence01/collisions/january.csv hdfs:///user/jspence01/collisions/compute.py

import pyspark
import csv

def extractTrips(partitionId,partition):
    if partitionId == 0:
        partition.next()
    import csv
    reader = csv.reader(partition)
    for row in reader:
        if row[1] and row[2] !='0' and row[6] and row [7] != '0':
            pickup = row[1],row[2]
            dropoff = row[6],row[7]
            yield (pickup,1)
            yield (dropoff,1)

def extractCyclists(partitionId,partition):
    if partitionId == 0:
        partition.next()
    import csv
    reader = csv.reader(partition)
    for row in reader:
        if row[14]!= '' and row[14] != '0':
            lat = row[6].split(',')[0][1:9] + '' , '' + row[6].split(',')[1][0:9]
            injured = int(row[14]) + int(row[15])    # includes injury and death
            yield (lat),injured

def toCSVLine(data):
    return ','.join(str(d) for d in data)

if __name__=='__main__':

    sc = pyspark.SparkContext()

    collisions = 'hdfs:///user/jspence01/collisions/collisions.csv'
    janTrips = 'hdfs:///user/jspence01/collisions/january.csv'
    febTrips = 'hdfs:///user/jspence01/collisions/february.csv'
    marchTrips = 'hdfs:///user/jspence01/collisions/march.csv'
    aprilTrips = 'hdfs:///user/jspence01/collisions/april.csv'
    mayTrips = 'hdfs:///user/jspence01/collisions/may.csv'
    juneTrips = 'hdfs:///user/jspence01/collisions/june.csv'

    janTrips = sc.textFile(janTrips,use_unicode=False).cache()
    febTrips = sc.textFile(febTrips,use_unicode=False).cache()
    marchTrips = sc.textFile(marchTrips,use_unicode=False).cache()
    aprilTrips = sc.textFile(aprilTrips,use_unicode=False).cache()
    mayTrips = sc.textFile(mayTrips,use_unicode=False).cache()
    juneTrips = sc.textFile(juneTrips,use_unicode=False).cache()
    collisions = sc.textFile(collisions,use_unicode=False).cache()

    janTrips = janTrips.mapPartitionsWithIndex(extractTrips).reduceByKey(lambda x,y: x+y)
    febTrips = febTrips.mapPartitionsWithIndex(extractTrips).reduceByKey(lambda x,y: x+y)
    marchTrips = marchTrips.mapPartitionsWithIndex(extractTrips).reduceByKey(lambda x,y: x+y)
    aprilTrips = aprilTrips.mapPartitionsWithIndex(extractTrips).reduceByKey(lambda x,y: x+y)
    mayTrips = mayTrips.mapPartitionsWithIndex(extractTrips).reduceByKey(lambda x,y: x+y)
    juneTrips = juneTrips.mapPartitionsWithIndex(extractTrips).reduceByKey(lambda x,y: x+y)

    jan_feb = febTrips.join(janTrips).mapValues(lambda x: x[0]+x[1])
    jan_march = marchTrips.join(jan_feb).mapValues(lambda x: x[0]+x[1])
    jan_april = aprilTrips.join(jan_march).mapValues(lambda x: x[0]+x[1])
    jan_may = mayTrips.join(jan_april).mapValues(lambda x: x[0]+x[1])
    jan_june = juneTrips.join(jan_may).mapValues(lambda x: x[0]+x[1])


    cyclistAccidents = collisions.mapPartitionsWithIndex(extractCyclists).reduceByKey(lambda x,y: x+y)
    total_data = jan_june.join(cyclistAccidents).collect()
    print total_data
    # with open('hdfs:///user/mhendri000/bike_taxi.csv', 'wb') as myfile:
    #     wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
    #     for i in range (0,len(total_data)):
    #         wr.writerow(total_data[i])

    # lines = total_data.map(toCSVLine)
    # lines.saveAsTextFile('hdfs:///user/mhendri000/trips_injuries.csv')
