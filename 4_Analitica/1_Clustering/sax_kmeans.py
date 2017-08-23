
import ast
import numpy as np
import pandas as pd

from operator import add
from scipy import stats

from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.mllib.linalg import Vectors, VectorUDT
from pyspark.sql.types import StringType, IntegerType, FloatType


class SAXKMeans():
    
    def __init__(self, featuresCol="features", predictionCol="prediction", k=2, numIntervals=2, alphabetSize=10,
                    tol=1e-4, maxIter=10, initialization="k++", seed=None):
        
        # store init variables
        self._numIntervals = numIntervals
        self._alphabetSize = alphabetSize
        self._featuresCol = featuresCol
        self._predictionCol = predictionCol
        self._k = k
        self._tol = tol
        self._maxIter = maxIter
        self._initialization = initialization
        self.seed = seed
        
        # check initialization variable
        if self._initialization == "random":
            self._centroid_init_function = self._random_init
        elif self._initialization == "k++":
            self._centroid_init_function = self._kpp_init
        else:
            raise ValueError("Param 'initialization' must be [random, k++]")
        
        # initialize output variables
        self.centroids_ = []
        self.cost = None
        
        # store alphabet
        self._alphabet = np.array(range(self._alphabetSize))
        
        # calculate discretization intervals
        perc = np.linspace(0,1,self._alphabetSize+1)[1:-1]
        self._intervalPoints = np.array([stats.norm.ppf(p,loc=0,scale=1) for p in perc])
        
        # calculate distances matrix
        distance_matrix = np.zeros((self._alphabetSize,self._alphabetSize))
        for i in range(self._alphabetSize):
            for j in range(self._alphabetSize):

                r = int(self._alphabet[i])
                c = int(self._alphabet[j])

                if abs(r-c)<=1:
                    distance_matrix[r,c] = 0
                else:
                    distance_matrix[r,c] = self._intervalPoints[max(r,c)-1] - self._intervalPoints[min(r,c)]
        
        self._distanceMatrix = pd.DataFrame(distance_matrix,index=self._alphabet,columns=self._alphabet)  
        
        # check if model is trained
        self._trained = False
        
        
    def fit(self, df):
        
        # check numIntervals <= serieLength
        self._seriesLength = len(df.select(self._featuresCol).first()[0])
        self._reductionCoefficient = self._seriesLength/float(self._numIntervals)
        if self._numIntervals > self._seriesLength:
            raise ValueError("Number of intervals {} couldn't be greater than series length {}".format(
                self._numIntervals,self._seriesLength))
        else:
            # make SAX transformation
            df = self._sax_transform(df)

            # fit kmeans algorithm
            df = self._fit(df)

            self._trained = True
        
        
    def transform(self, df):
        
        # dataframe columns
        cols = df.columns
        
        # make SAX transformation
        df = self._sax_transform(df)
        
        # calculate distance to centroids
        distance_to_centroids_udf = F.udf(lambda x: Vectors.dense(self._distance_to_centroids(x.toArray().astype(int))),
                                            returnType=VectorUDT())
        df = df.withColumn("dist_centroids",distance_to_centroids_udf(df["discretized_serie"]))

        # assignation
        min_distance_udf = F.udf(lambda x: int(np.argmin(x.toArray())),returnType=IntegerType())
        df = df.withColumn("assignation",min_distance_udf(df["dist_centroids"]))
        
        # return prediction dataframe
        return df.select(cols+["assignation"])
        
    
    def _sax_transform(self, df):
        
        # normalize series
        normalize_udf = F.udf(lambda x: Vectors.dense((x.toArray()-np.mean(x.toArray()))/np.std(x.toArray())),
                                    returnType=VectorUDT())
        df = df.withColumn("normalized_serie",normalize_udf(df[self._featuresCol]))
        
        # piecewise aggregate aproXimation (PAA)
        to_paa_udf = F.udf(lambda x: Vectors.dense(self._to_paa(x.toArray())),
                            returnType=VectorUDT())
        df = df.withColumn("paa_serie",to_paa_udf(df["normalized_serie"]))       
        
        # discretization
        discretize_udf = F.udf(lambda x: Vectors.dense(self._discretize(x.toArray())),
                             returnType=VectorUDT())
        df = df.withColumn("discretized_serie",discretize_udf(df["paa_serie"]))
        
        return df
    
    
    def _fit(self, df):
        
        self._centroid_init_function(df)
        
        # fit kmeans algorithm
        cost_values = [np.inf]
        for it in range(self._maxIter):
                                
            # calculate distance to centroids
            distance_to_centroids_udf = F.udf(lambda x: Vectors.dense(self._distance_to_centroids(x.toArray().astype(int))),
                                                returnType=VectorUDT())
            df = df.withColumn("dist_centroids",distance_to_centroids_udf(df["discretized_serie"]))

            # assignation
            min_distance_udf = F.udf(lambda x: int(np.argmin(x.toArray())),returnType=IntegerType())
            df = df.withColumn("assignation",min_distance_udf(df["dist_centroids"]))

            # recalculate centroids
            df_centroids = df.select(["assignation","paa_serie","dist_centroids"])
            centroids_samples = df_centroids.map(lambda x: (x[0],(x[1].toArray(),1,x[2].toArray()[x[0]])))
            centroids_sum = centroids_samples.reduceByKey(lambda x,y: (np.add(x[0],y[0]),x[1]+y[1],x[2]+y[2]))
            centroids_mean = centroids_sum.map(lambda x: (x[0],x[1][0]/float(x[1][1]))).collect()
            centroids_mean = sorted(centroids_mean)
            for i,centroid in centroids_mean:
                self.centroids_[i] = self._discretize(centroid)

            # calculate cost
            cost_sum = centroids_sum.map(lambda x: x[1][2]).reduce(add)
            
            # check for convergence
            if abs(cost_values[-1] - cost_sum) <= self._tol:
                # convergence reached
                cost_values.append(cost_sum)
                break
                
            cost_values.append(cost_sum)
            
        self.cost_ = cost_values[1:]
        return df


    def _random_init(self, df):
        
        samples = df.select("discretized_serie").sample(False,0.2).take(self._k)
        self.centroids_ = [sample["discretized_serie"].toArray().astype(int) for sample in samples]
    
    
    def _kpp_init(self, df):

        self.centroids_ = []
        new_centroid = df.select("discretized_serie").sample(False,0.5).first()[0]
        self.centroids_.append(new_centroid.toArray())

        sw = True
        while(sw):
            df_aux = df.select("discretized_serie")
            
            # calculate distance to centroids
            distance_to_centroids_udf = F.udf(lambda x: Vectors.dense(self._distance_to_centroids(x.toArray().astype(int))),
                                                returnType=VectorUDT())
            df_aux = df_aux.withColumn("dist_centroids",distance_to_centroids_udf(df_aux["discretized_serie"]))

            # calculate assignation
            min_distance_udf = F.udf(lambda x: int(np.argmin(x.toArray())),returnType=IntegerType())
            df_aux = df_aux.withColumn("assignation",min_distance_udf(df_aux["dist_centroids"]))

            # distance to nearest centroid
            nearest_centroid_udf = F.udf(lambda x: float(np.amin(x.toArray())),returnType=FloatType())
            df_aux = df_aux.withColumn("dist_nearest_centroid",nearest_centroid_udf(df_aux["dist_centroids"]))
            
            # order centroids by distance
            df_aux = df_aux.withColumn("dist_nearest_centroid_reversed",(-1)*df_aux["dist_nearest_centroid"])
            window = Window.partitionBy("assignation").orderBy("dist_nearest_centroid_reversed")
            df_aux = df_aux.select(df_aux["discretized_serie"],df_aux["dist_nearest_centroid_reversed"],F.row_number().over(window).alias("ordering"))
            df_aux = df_aux.where(df_aux["ordering"]==4)
            
            # get new centroids
            new_centroids = df_aux.select("discretized_serie").collect()
            for new_centroid in new_centroids:
                self.centroids_.append(new_centroid["discretized_serie"].toArray())
                if len(self.centroids_)>=self._k:
                    sw = False
                    break
                    
        self.centroids_ = [centroid.astype(int) for centroid in self.centroids_]
    
    
    def _distance_to_centroids(self, word):

        distances = []
        for centroid in self.centroids_:
            distances.append(self._mindist(word,centroid))

        return np.array(distances)
    
    
    def _to_paa(self, vector):

        # split vector in intervals
        vectors = np.array_split(vector, self._numIntervals)

        # calculate mean values
        means = []
        for v in vectors:
            means.append(np.mean(v))

        return np.array(means)
        
    def _discretize(self, vector):

        return self._alphabet[1-np.digitize(vector,self._intervalPoints)]
    
    
    def _mindist(self, word1, word2):

        return np.sqrt(self._reductionCoefficient)*np.sqrt(np.sum(self._dist(word1,word2)**2))
    
    
    def _dist(self, word1, word2):

        distances = []
        letter_pairs = zip(word1.tolist(),word2.tolist())

        for letter1,letter2 in letter_pairs:
            distances.append(self._distanceMatrix.ix[letter1,letter2])

        return np.array(distances)
    
