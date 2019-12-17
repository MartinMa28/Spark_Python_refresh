import sys
from pyspark import SparkConf, SparkContext
from math import sqrt

def load_movies_names():
    movie_names = {}

    with open('../../ml-100k/u.item') as f:
        for line in f:
            fields = line.split('|')
            movie_names[int(fields[0])] = fields[1]

    return movie_names


def computeCosineSimilarity(ratingPairs):
    numPairs = 0
    sum_xx = sum_yy = sum_xy = 0
    for ratingX, ratingY in ratingPairs:
        sum_xx += ratingX * ratingX
        sum_yy += ratingY * ratingY
        sum_xy += ratingX * ratingY
        numPairs += 1

    numerator = sum_xy
    denominator = sqrt(sum_xx) * sqrt(sum_yy)

    score = 0
    if (denominator):
        score = (numerator / (float(denominator)))

    return (score, numPairs)


def make_pairs(data):
    user, ratings = data
    movie1, rating1 = ratings[0]
    movie2, rating2 = ratings[1]

    return ((movie1, movie2), (rating1, rating2))



conf = SparkConf().setMaster('local[*]').setAppName('MovieSimilarities')
sc = SparkContext(conf=conf)

id_2_movie_name = load_movies_names()

data = sc.textFile('file:///home/martin/Documents/spark_udemy_tutorial/ml-100k/u.data')

# Map the ratings to key / value pairs: user ID => movie ID, rating
ratings = data.map(lambda l: l.split('\t')).map(lambda l: (int(l[0]), (int(l[1]), float(l[2]))))

# self-join to find every combination
joined_ratings = ratings.join(ratings)
# At this time, our RDD consists of user ID => ((movie ID, rating), (movie ID, rating))

# Filter out duplicated pairs
non_dup_joined_ratings = joined_ratings.filter(lambda data: data[1][0][0] < data[1][1][0])

# Make movie pairs
movie_pairs = non_dup_joined_ratings.map(make_pairs)
# Group the movie pairs rated by all of different users
movie_pair_ratings = movie_pairs.groupByKey()
movie_pair_similarities = movie_pair_ratings.mapValues(computeCosineSimilarity).cache()

if __name__ == "__main__":
    for movie_id in sys.argv[1:]:
        movie_id = int(movie_id)
        score_thresh = 0.97
        co_occurence_thresh = 50

        filtered_results = movie_pair_similarities.filter(lambda pair_simi: \
            (pair_simi[0][0] == movie_id or pair_simi[0][1] == movie_id) and \
            pair_simi[1][0] > score_thresh and pair_simi[1][1] > co_occurence_thresh)
        
        sorted_results = filtered_results.sortBy(lambda data: data[1][0], ascending=False).take(10)

        print('Top 10 similar movies for {}'.format(id_2_movie_name[movie_id]))
        for result in sorted_results:
            pair, similarity = result

            similar_id = pair[0]
            if pair[0] == movie_id:
                similar_id = pair[1]

            print(id_2_movie_name[similar_id] + '\tscore: {}, strength: {}'.format(
                str(similarity[0]), similarity[1]))






