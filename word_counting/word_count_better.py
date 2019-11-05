import re
from pyspark import SparkContext, SparkConf

def normalize_words(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

if __name__ == "__main__":
    conf = SparkConf().setMaster('local').setAppName('BetterWordCount')
    sc = SparkContext(conf=conf)

    book = sc.textFile('file:///home/martin/Documents/spark_udemy_tutorial/PySpark_tutorial/word_counting/book.txt')
    words = book.flatMap(normalize_words)
    word_counts = words.map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b)
    sorted_word_counts = word_counts.sortBy(lambda x: x[1], ascending=False)
    
    results = sorted_word_counts.take(20)

    for result in results:
        print('{}: {}'.format(str(result[0]), str(result[1])))

