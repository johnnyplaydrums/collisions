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


if __name__=='__main__':

    sc = pyspark.SparkContext()

    collisions = '/user/jspence01/collisions/collisions.csv'
    januaryTrips = '/user/jspence01/collisions/1january.csv'
    febTrips = 'users/jspence01/collisions/2february.csv'
    marchTrips = '/user/jspence01/collisions/3march.csv'
    aprilTrips = '/user/jspence01/collisions/4april.csv'
    mayTrips = '/user/jspence01/collisions/5may.csv'
    juneTrips = '/user/jspence01/collisions/6june.csv'

    janTrips = sc.textFile(janTrips,use_unicode=False).cache()
    febTrips = sc.textFile(febTrips,use_unicode=False).cache()
    marchTrips = sc.textFile(marchTrips,use_unicode=False).cache()
    aprilTrips = sc.textFile(aprilTrips,use_unicode=False).cache()
    mayTrips = sc.textFile(mayTrips,use_unicode=False).cache()
    juneTrips = sc.textFile(juneTrips,use_unicode=False).cache()

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
    jan_june.join(cyclistAccidents).collect()
