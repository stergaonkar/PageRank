import argparse
import re
import sys
from operator import add
from pyspark.sql import SparkSession

def computeContributions(nodes, rank):
    """Calculates node contributions to the rank of other nodes."""
    total_nodes = len(nodes)
    for node in nodes:
        yield (node, rank / total_nodes)


def pageRank(spark, input_file, iterations, partitions):

    # Loading in input_file. It should be in format of: node, neighbor node
    dataSet = spark.sparkContext.textFile(input_file)
    # Loading all nodes from input file and initializes the neighbor nodes.
    pairs = dataSet.map(lambda x: tuple([float(n) for n in re.split('[ |\t]', x)])).distinct().groupByKey()
    # Loading all nodes with other node(s) link to from input_file and initializes ranks of every node to 1
    ranks = pairs.mapValues(lambda x: 1.0)
   

    # Calculate/Update node ranks with PageRank algorithm continuously
    for iteration in range(iterations):
        # Calculates node contributions to the rank of other nodes.
        node_contrib = pairs.join(ranks).flatMap(
            lambda nodes_rank: computeContributions(nodes_rank[1][0], nodes_rank[1][1]))
        ranks = node_contrib.reduceByKey(add).mapValues(lambda x: x * 0.85 + 0.15)

       
    #Sorting Ranks in descending order
    #sorted_ranks = ranks.top(3, key=lambda x: x[1])
    #print("!!!!!!!!  PRINTING TOP 3 NODES!!!!! ",sorted_ranks)
    #Saving the output to HDFS server
    ranks.saveAsTextFile('PR_for_iterations'+str(iterations)+'.txt')


def pageRank_Partitions(spark, input_file, iterations, partitions):

    # Loading in input_file. It should be in format of: node, neighbor node
    dataSet = spark.sparkContext.textFile(input_file,partitions)
    # Loading all nodes from input file and initializes the neighbor nodes.
    pairs = dataSet.map(lambda x: tuple([float(n) for n in re.split('[ |\t]', x)])).distinct().groupByKey().partitionBy(partitions)
    # Loading all nodes with other node(s) link to from input_file and initializes ranks of every node to 1
    ranks = pairs.mapValues(lambda x: 1.0).partitionBy(partitions)
    ranks = ranks.partitionBy(partitions)

    # Calculate/Update node ranks with PageRank algorithm continuously
    for iteration in range(iterations):
        # Calculates node contributions to the rank of other nodes.
        node_contrib = pairs.join(ranks).flatMap(
            lambda nodes_rank: computeContributions(nodes_rank[1][0], nodes_rank[1][1]))
        ranks = node_contrib.reduceByKey(add).mapValues(lambda x: x * 0.85 + 0.15).partitionBy(partitions)
       
       
    #Sorting Ranks in descending order
    #sorted_ranks = ranks.top(3, key=lambda x: x[1])
    #print("!!!!!!!!  PRINTING TOP 3 NODES!!!!! ",sorted_ranks)
    #Saving the output to HDFS server
    ranks.saveAsTextFile('PR_with_partitions_for_iterations'+str(iterations)+'.txt')

def pageRank_Cache(spark, input_file, iterations, partitions):

    # Loading in input_file. It should be in format of: node, neighbor node
    dataSet = spark.sparkContext.textFile(input_file,partitions)
    # Loading all nodes from input file and initializes the neighbor nodes.
    pairs = dataSet.map(lambda x: tuple([float(n) for n in re.split('[ |\t]', x)])).distinct().groupByKey().partitionBy(partitions).cache()
    # Loading all nodes with other node(s) link to from input_file and initializes ranks of every node to 1
    ranks = pairs.mapValues(lambda x: 1.0).partitionBy(partitions)
    ranks = ranks.partitionBy(partitions)

    # Calculate/Update node ranks with PageRank algorithm continuously
    for iteration in range(iterations):
        # Calculates node contributions to the rank of other nodes.
        node_contrib = pairs.join(ranks).flatMap(
            lambda nodes_rank: computeContributions(nodes_rank[1][0], nodes_rank[1][1]))
        ranks = node_contrib.reduceByKey(add).mapValues(lambda x: x * 0.85 + 0.15).partitionBy(partitions).cache()


    #Sorting Ranks in descending order
    #sorted_ranks = ranks.top(3, key=lambda x: x[1])
    #print("!!!!!!!!  PRINTING TOP 3 NODES!!!!! ",sorted_ranks)
    #Saving the output to HDFS server
    ranks.saveAsTextFile('PR_with_cache_for_iterations'+str(iterations)+'.txt')


def sparkContext(input_file, iterations, partitions):
    # Initialize the spark context.
    spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .config("spark.driver.memory","1g")\
    	.config("spark.eventLog.enabled","true")\
    	.config("spark.executor.memory","1g")\
    	.config("spark.executor.cores","8")\
    	.config("spark.task.cpus","1")\
    	.config("spark.executor.instances","16")\
    	.config("spark.default.parallellism","16")\
        .getOrCreate()

    #call to Pagerank.
    pageRank_Partitions(spark, input_file, iterations, partitions)
    
    #stop the sessiom
    spark.stop()

if __name__ == "__main__":

    #Iterations and Input_file are passed as arguments
    parser = argparse.ArgumentParser(description = 'PageRank Computation', formatter_class=argparse.ArgumentDefaultsHelpFormatter)   
    parser.add_argument('-i', '--input', help='Input file: DataSet') 
    parser.add_argument('-p', '--partitions',default=16, help='number of partitions')
    parser.add_argument('-itr', '--iterations', help='Iterations to calculate node ranks')

    args = parser.parse_args()
    
    #Reading Input_file
    if args.input:
        input_file = args.input

    #Reading number of partitions
    if args.partitions:
        partitions = int(args.partitions)

    #Reading Number of Iterations
    if args.iterations:
        iterations = int(args.iterations)
  
    
    #Function call to initialize sparkContext and run the pageRank Algorithm
    sparkContext(input_file, iterations, partitions)   
