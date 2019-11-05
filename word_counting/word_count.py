import re
from pyspark import SparkContext, SparkConf

def normalize_words(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

if __name__ == "__main__":
    conf = SparkConf().setMaster('local').setAppName('WordCount')
    sc = SparkContext(conf=conf)

    book = sc.textFile('file:///home/martin/Documents/spark_udemy_tutorial/PySpark_tutorial/word_counting/book.txt')
    words = book.flatMap(normalize_words)

    results = words.countByValue()
    
    c = 0
    for word, count in results.items():
        print(str(word), count)
        c += count

    print(cnt, c)

    