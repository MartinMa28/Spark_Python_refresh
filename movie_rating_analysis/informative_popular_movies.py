from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('PopularMovies')
sc = SparkContext(conf=conf)


def load_move_names():
    movie_names = {}
    with open('/home/martin/Documents/spark_udemy_tutorial/ml-100k/u.item') as f:
        for line in f:
            fields = line.split('|')
            movie_names[int(fields[0])] = fields[1]

    return movie_names


def sort_by_popularity(file_path):
    ratings = sc.textFile(file_path)
    movies_ids = ratings.map(lambda line: (int(line.split('\t')[1]), 1))
    counts = movies_ids.reduceByKey(lambda x, y: x + y)
    sorted_counts = counts.sortBy(lambda id_cnt: id_cnt[1])
    sorted_movie_with_names = sorted_counts.map(\
        lambda (movie_id, cnt): (id_name.value[movie_id], cnt))
    
    results = sorted_movie_with_names.collect()

    return results

if __name__ == "__main__":
    id_name = sc.broadcast(load_move_names())
    results = sort_by_popularity('/home/martin/Documents/spark_udemy_tutorial/ml-100k/u.data')

    for movie_name, cnt in results:
        print(movie_name, cnt)