import pyspark
import csv

def extractTrips(partitionId,partition):
    if partitionId == 0:
        partition.next()
    import csv
    reader = csv.reader(partition)
    for row in reader:
        pickup = row[1],row[2]
        dropoff = row[6],row[7]
        yield (pickup,1)
        yield (dropoff,1)



if __name__=='__main__':

    sc = pyspark.SparkContext()

    collisions = '/user/jspence01/collisions/collisions.csv'
    januaryTrips = '/user/jspence01/collisions/1january.csv'
    marchTrips = '/user/jspence01/collisions/3march.csv'
    aprilTrips = '/user/jspence01/collisions/4april.csv'
    mayTrips = '/user/jspence01/collisions/5may.csv'
    juneTrips = '/user/jspence01/collisions/6june.csv'

    marchTrips = sc.textFile(marchTrips,use_unicode=False).cache()
    aprilTrips = sc.textFile(aprilTrips,use_unicode=False).cache()
    mayTrips = sc.textFile(mayTrips,use_unicode=False).cache()
    juneTrips = sc.textFile(juneTrips,use_unicode=False).cache()

    janTrips = january.mapPartitionsWithIndex(extractTrips)
    febTrips = february.mapPartitionsWithIndex(extractTrips)
    marchTrips = marchTrips.mapPartitionsWithIndex(extractTrips)
    aprilTrips = aprilTrips.mapPartitionsWithIndex(extractTrips)
    mayTrips = mayTrips.mapPartitionsWithIndex(extractTrips)
    juneTrips = juneTrips.mapPartitionsWithIndex(extractTrips)

    jan_feb = febTrips.join(taxiTrips).reduceByKey(lambda x,y: x+y).mapValues(lambda x: x[0]+x[1])
    jan_march = marchTrips.join(jan_feb).reduceByKey(lambda x,y: x+y).mapValues(lambda x: x[0]+x[1])
    jan_april = aprilTrips.join(jan_march).reduceByKey(lambda x,y: x+y).mapValues(lambda x: x[0]+x[1])
    jan_may = mayTrips.join(jan_april).reduceByKey(lambda x,y: x+y).mapValues(lambda x: x[0]+x[1])
    jan_june = juneTrips.join(jan_may).reduceByKey(lambda x,y: x+y).mapValues(lambda x: x[0]+x[1])

    jan_june.take(15)
