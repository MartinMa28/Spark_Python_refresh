from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster('local').setAppName('TheMostPopularMovie')
sc = SparkContext(conf=conf)

def sort_by_popularity(file_path):
    ratings = sc.textFile(file_path)
    movie_ids = ratings.map(lambda line: (line.split('\t')[1], 1))
    counts = movie_ids.reduceByKey(lambda x, y: x + y)
    sorted_counts = counts.sortBy(lambda id_cnt: id_cnt[1])

    sorted_counts = sorted_counts.collect()

    return sorted_counts

if __name__ == "__main__":
    cnts = sort_by_popularity('file:///home/martin/Documents/spark_udemy_tutorial/ml-100k/u.data')

    for movie_id, cnt in cnts:
        print(movie_id, cnt)