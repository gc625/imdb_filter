# IMDB filtering algorithms

This repo contains scripts written in Scala to process the raw info about movies and actors from the official IMDB databas and algorithms to perform certain tasks on the database.

The dataset is loaded as Resilient Distributed Datasets (RDDs) and processes using MapReduce style functions to process the data in a scalable manner for distributed processing.


## Sample Functions
Example of functions implemented. They are named `task{n}` respectively in the code.

### Task 1
Calculate the average, minimum, and maximum runtime duration for all titles per movie genre

### Task 2
Return the titles of the movies which were released between 1990 and 2018 (inclusive), have an average rating of 7.5 or more, and have received 500000 votes or more.

### Task 3
Return the top rated movie of each genre for each decade between 1900 and 1999.

### Task 4
Returns all the crew names (primaryName) for whom there are at least two known for films released since the year 2010 up to and including the year (2021).
