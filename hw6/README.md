# CS 290: Data Engineering HW 1
Written by Cody Lieu (cal53)

## Running Notes
- Note sure if this matters, but I put all the .csv and .txt data files on the top level of this directory and refer to them with the appropriate relative path compared to the top level of the directory.
- For Part B, I had to run spark-submit or spark-shell with the '--driver-memory 4g' option to make it work. Before using this option, I was getting OutOfMemoryError: GC overhead limit exceeded.
- I saw that Professor Babu had WhiteHouses.scala and Wikipedia.scala as the file files in the src dir. I wasn't sure how strictly you wanted us to adhere to that, so I hope that putting each individual part into its own file is okay.
- The output format wasn't specified and I wasn't sure if we should aggregate some redundant entries (like all the different names for POTUS/FLOTUS), so I made some assumptions in how I output my answers.

## Answers

#### Part A

##### i

1. ((Hash,Michael,M),775)
2. ((Borzi,Phyllis,C),526)
3. ((Tavenner,Marilyn,n),497)
4. ((Levitis,Jason,A),488)
5. ((Hoff,James,C),486)
6. ((Mann,Cynthia,R),398)
7. ((Schultz,William,B),395)
8. ((Khalid,Aryana,C),395)
9. ((BrooksLaSure,Chiquita,n),390)
10. ((Bowler,Timothy,J),383)

##### ii

1. ((OFFICE,VISITORS),2889673)
2. ((,POTUS),130610)
3. ((POTUS,),75855)
4. ((,POTUS/FLOTUS),38273)
5. ((,),24745)
6. ((Lambrew,Jeanne),18067)
7. ((Lierman,Kyle),17965)
8. ((,FLOTUS),14849)
9. ((,potus),14715)
10. ((OFFICE,VISITORS ),12063)


##### iii

1. (((Hash,Michael,M),(Lambrew,Jeanne)),583)
2. (((Tavenner,Marilyn,n),(Lambrew,Jeanne)),422)
3. (((Hoff,James,C),(Hoff,Joanne)),407)
4. (((Levitis,Jason,A),(Lambrew,Jeanne)),359)
5. (((BrooksLaSure,Chiquita,n),(Lambrew,Jeanne)),346)
6. (((Mann,Cynthia,R),(Lambrew,Jeanne)),332)
7. (((Khalid,Aryana,C),(Lambrew,Jeanne)),326)
8. (((Fontenot,Yvette,E),(Lambrew,Jeanne)),313)
9. (((Borzi,Phyllis,C),(Lambrew,Jeanne)),310)
10. (((Turner,Amy,J),(Lambrew,Jeanne)),303)

#### Part B
I just outputted the count since I could check it with the results from the url and it wouldn't be efficient to print out all entries

##### i
- Count of RDD: 10,738
- This answer was off by 300 from the website, but I think this is a typo on his part

##### ii
- Count of RDD: 1,942,943
