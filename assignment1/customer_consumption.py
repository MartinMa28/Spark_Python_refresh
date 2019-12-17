from pyspark import SparkConf, SparkContext

def get_spark_context():
    conf = SparkConf().setMaster('local').setAppName('TotalConsumption')
    sc = SparkContext(conf=conf)

    return sc


def get_total_consumption(sc, file_path):
    lines = sc.textFile('file://' + file_path)
    orders = lines.map(lambda line : [col.strip() for col in line.split(',')])
    customer_spend = orders.map(lambda c: (c[0], float(c[2])))
    results = customer_spend.reduceByKey(lambda a, b: a + b)
    results = results.collect()

    for k, v in results:
        print('User ID: {:2}, Total consumption: {:8.2f}$'.format(k, v))


if __name__ == "__main__":
    sc = get_spark_context()
    get_total_consumption(sc, 
    '/home/martin/Documents/MartinMa/spark_tutorial/assignment1/customer-orders.csv')
