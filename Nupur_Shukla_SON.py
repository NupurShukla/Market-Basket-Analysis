from pyspark import SparkContext
from collections import Counter
from itertools import combinations
import csv
import sys
import time
import os
import math

def candidateCount(baskets, candidates):
	itemCountDict = {}
	baskets = list(baskets)

	for candidate in candidates:
		if type(candidate) is int:
			candidate = [candidate]
			key = tuple(sorted(candidate))
		else:
			key = candidate

		candidate = set(candidate)
		for basket in baskets:
			if candidate.issubset(basket):
				if key in itemCountDict:
					itemCountDict[key] = itemCountDict[key] + 1
				else:
					itemCountDict[key] = 1

	return itemCountDict.items()

def getFrequentItems(baskets, candidates, threshold):
	kItemCountDict = {}
	for candidate in candidates:
		candidate = set(candidate)
		key = tuple(sorted(candidate))

		for basket in baskets:
			if candidate.issubset(basket):
				if key in kItemCountDict:
					kItemCountDict[key] = kItemCountDict[key] + 1
				else:
					kItemCountDict[key] = 1

	kItemCount = Counter(kItemCountDict)
	kFrequentItems = {x : kItemCount[x] for x in kItemCount if kItemCount[x] >= threshold }
	kFrequentItems = sorted(kFrequentItems)

	return kFrequentItems

def getCandidatePairs(frequentItems):
	pairs = list()
	for subset in combinations(frequentItems, 2):
		subset = list(subset)
		subset.sort()
		pairs.append(subset)

	return pairs

def getCandidateItems(frequentItems, k):
	combinations1 = list()
	frequentItems = list(frequentItems)

	for i in range(len(frequentItems)-1):
		for j in range(i+1, len(frequentItems)):
			a = frequentItems[i]
			b = frequentItems[j]
			if a[0:(k-2)] == b[0:(k-2)]:
				combinations1.append(list(set(a) | set(b)))
			else:
				break
	return combinations1
			
def apriori(baskets, support, totalCount):
	baskets = list(baskets)
	threshold = support*(float(len(baskets))/float(totalCount))
	#threshold = math.floor(threshold)
	finalResult = list()

	singleItemCount = Counter()
	for basket in baskets:
		singleItemCount.update(basket)

	candidateSingletons = {x : singleItemCount[x] for x in singleItemCount if singleItemCount[x] >= threshold }
	frequentSingletons = sorted(candidateSingletons)
	finalResult.extend(frequentSingletons)

	k=2
	frequentItems = set(frequentSingletons)
	while len(frequentItems) != 0:
		if k==2:
			candidateFrequentItems = getCandidatePairs(frequentItems)
		else:
			candidateFrequentItems = getCandidateItems(frequentItems, k)

		newFrequentItems = getFrequentItems(baskets, candidateFrequentItems, threshold)
		finalResult.extend(newFrequentItems)
		frequentItems = list(set(newFrequentItems))
		frequentItems.sort()
		k=k+1

	return finalResult

start = time.time()

case = int(sys.argv[1])
inputFile = sys.argv[2]
support  = int(sys.argv[3])

#case=1
#inputFile="/Users/nupur/Desktop/DataMining/Assignments/Assignment_02/Data/Small1.csv"
#support=4

fileNameWithExt = os.path.basename(inputFile)
fileName = os.path.splitext(fileNameWithExt)[0]
outputFile = "Nupur_Shukla_SON_" + fileName + ".case" + str(case) + "-" + str(support) + ".txt"

sc = SparkContext("local[*]", "SON")
rdd = sc.textFile(inputFile, minPartitions=None, use_unicode=False)
rdd = rdd.mapPartitions(lambda x : csv.reader(x))

header = rdd.first()
rdd = rdd.filter(lambda x : x != header)

if case==1:
	allBaskets = rdd.map(lambda x : (int(x[0]), int(x[1]))).groupByKey().mapValues(set)

elif case==2:
	allBaskets = rdd.map(lambda x : (int(x[1]), int(x[0]))).groupByKey().mapValues(set)


allBaskets = allBaskets.map(lambda x : x[1])
totalCount = allBaskets.count()

#Phase 1
mapOutput1 = allBaskets.mapPartitions(lambda baskets : apriori(baskets, support, totalCount)).map(lambda x : (x, 1))
reduceOutput1 = mapOutput1.reduceByKey(lambda x,y: (1)).keys().collect()

#Phase 2
mapOutput2 = allBaskets.mapPartitions(lambda baskets : candidateCount(baskets, reduceOutput1))
reduceOutput2 = mapOutput2.reduceByKey(lambda x,y: (x+y))

final = reduceOutput2.filter(lambda x: x[1] >= support)

#Writing to file
outFile = open(outputFile, "w")
frequentItems = final.keys().collect()
frequentItems = sorted(frequentItems, key = lambda item: (len(item), item))

if len(frequentItems) != 0:
	pSize = len(frequentItems[0])
	strVal = str(frequentItems[0]).replace(',', '')
	outFile.write(strVal)
	for i in range(1, len(frequentItems)):
		cSize = len(frequentItems[i])
		if pSize == cSize:
			outFile.write(", ")
		else:
			outFile.write("\n\n")

		if cSize == 1:
			strVal = str(frequentItems[i]).replace(',', '')
		else:
			strVal = str(frequentItems[i])
		outFile.write(strVal)
		pSize = cSize

end = time.time()
print "Time taken: ", end - start, " seconds"