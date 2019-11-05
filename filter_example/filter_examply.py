from pyspark import SparkConf, SparkContext

def parse_line(line):
    cols = list(map(lambda col: col.strip(), line.split(',')))
    station_id = cols[0]
    entry_type = cols[2]
    temperature = float(cols[3]) * 0.1 * (9.0 / 5.0) + 32.0

    return (station_id, entry_type, temperature)


if __name__ == "__main__":
    conf = SparkConf().setMaster('local').setAppName('TemperatureFilter')
    sc = SparkContext(conf=conf)

    lines = sc.textFile('file:///home/martin/Documents/spark_udemy_tutorial/PySpark_tutorial/filter_example/1800.csv')
    parsed_lines = lines.map(parse_line)
    min_temps = parsed_lines.filter(lambda line: line[1] == 'TMIN')
    station_temps = min_temps.map(lambda x: (x[0], x[2]))
    min_temps = station_temps.reduceByKey(lambda x, y: min(x, y))

    results = min_temps.collect()

    for result in results:
        print(result)
