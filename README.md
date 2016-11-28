# MovieRecommendationCosine

Description: 
1. Gathered MovieLens dataset consisting of 1 million ratings from 6000 users on 4000 movies, released before 2003. 
2. Performed Item Based collaborative searching for sorting the data for a unique movie rated by multiple users. 
3. Implemented Cosine similarity for calculating the similarity score based on user rating. 
4. Designed MapReduce code flow with multiple Mappers and Reducers using Python programming language to gather the recommended movie data for every movie selection.

Requirements: Pc or Laptop with minimum 4 GB Ram. Python and MRjob installed, no need to setup hadoop cluster.

Commands to run the job: python MovieRecommendationCosine.py --items=ml-1m/movies.dat ml-1m/ratings.dat > outputCosine.txt

Particular Movie output: I had designed a file for Star Wars movie after runnign the first MapReduce job, run the second job using:-python Star-Wars.py outputCosine.txt
