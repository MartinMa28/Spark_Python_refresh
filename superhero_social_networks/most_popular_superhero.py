from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('PopularHero')
sc = SparkContext(conf=conf)

def count_co_occurences(line):
    elements = line.split()
    return (int(elements[0]), len(elements) - 1)

def parse_name(line):
    fields = line.split('\"')
    return (int(fields[0]), fields[1])

names = sc.textFile('file:///home/martin/Documents/spark_udemy_tutorial/Spark_Python_refresh/datasets/Marvel-Names.txt')
name_rdd = names.map(parse_name)

lines = sc.textFile('file:///home/martin/Documents/spark_udemy_tutorial/Spark_Python_refresh/datasets/Marvel-Graph.txt')

pairings = lines.map(count_co_occurences)
id_cnts = pairings.reduceByKey(lambda x, y: x + y)
most_popular = id_cnts.max(lambda id_cnt: id_cnt[1])
most_popular_name = name_rdd.lookup(most_popular[0])[0]

print(most_popular_name + ' is the most popular superhero, with {} co-occurences.'.format(most_popular[1]))

