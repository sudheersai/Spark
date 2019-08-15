from pyspark import SparkContext,SparkConf
import configparser as cp ## to read the configaratin file
import sys ## it has arrgv to facilitates to read the arguments

props = cp.RawConfigParser()
props.read("../../resources/application.properties") ### it will create the dictonary format key, value paris
env = sys.argv[1] ## to get the dev properties

conf = SparkConf().\
    setMaster(props.get(env,'executionMode')).\
    setAppName('Daily Revenue') .\
    set("conf.ui.port", "12901")

sc = SparkContext(conf=conf)

print (props.get(env, 'input.base.dir') + "/" + "orders")

orders = sc.textFile(props.get(env, 'input.base.dir') + "/orders")
orderItems = sc.textFile(props.get(env, 'input.base.dir') + "/order_items")
print (orders.count())

ordersFiltered = orders.filter(lambda o: o.split(",")[3] in ("COMPLETE", "CLOSED"))
ordersFilteredMap = ordersFiltered.map(lambda o: (int(o.split(",")[0]), o.split(",")[1]))
orderItemsMap = orderItems.map(lambda oi: (int(oi.split(",")[1]), float(oi.split(",")[4])))
ordersJoin = ordersFilteredMap.join(orderItemsMap)
ordersJoinMap = ordersJoin.map(lambda o: o[1])
dailyRevenue = ordersJoinMap.reduceByKey(lambda x, y: x + y)
dailyRevenueSorted = dailyRevenue.sortByKey()
dailyRevenueSortedMap = dailyRevenueSorted.map(lambda oi: oi[0] + "," + str(oi[1]))
dailyRevenueSortedMap.saveAsTextFile(props.get(env, 'output.base.dir') + "/Daily_Revenue_App")

