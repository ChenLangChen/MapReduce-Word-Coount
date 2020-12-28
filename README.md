This MapReduce program is written in Java and deployed over AWS EMR

### Task 1 – Count words by lengths
Write a MapReduce program to count number of short words (1-4 letters), medium words (5-7 letters) words, long words (8-10 letters) and extra-long words (More than 10 letters).

### Task 2 – Count words by the first character
Write a MapReduce program that outputs a count of all words that begin with a vowel and count of all how many words that begin with a consonant.

### Task 3 – Count word with in-mapper combining
Write a MapReduce program to count the number of each word where the in-mapper combining is implemented rather than an independent combiner.

### Task 4 – Count word with partitioner 
Extend the MapReduce code in Task 1 by using partitioner such that
- short words (1-4 letters) and extra-long words (More than 10 letters) are processed in one reducer,
- medium words (5-7 letters) and long words (8-10 letters) are processed in another reducer.
