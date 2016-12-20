# cd spark1.5.1
# bin/pyspark

# Taken from: http://stackoverflow.com/questions/33428589/pyspark-and-pca-how-can-i-extract-the-eigenvectors-of-this-pca-how-can-i-calcu/33500704#33500704

# Test Example
from pyspark.ml.feature import *
from pyspark.mllib.linalg import Vectors

data = [(Vectors.dense([0.0, 1.0, 0.0, 7.0, 0.0]),), (Vectors.dense([2.0, 0.0, 3.0, 4.0, 5.0]),), (Vectors.dense([4.0, 0.0, 0.0, 6.0, 7.0]),)]

# The following two lines are not in the original implementation 
from pyspark.sql import  SQLContext
sqlContext=SQLContext(sc)
df = sqlContext.createDataFrame(data,["features"])

pca_extracted = PCA(k=2, inputCol="features", outputCol="pca_features")

model = pca_extracted.fit(df)
features = model.transform(df) # this create a DataFrame with the regular features and pca_features

# We can now extract the pca_features to prepare our RowMatrix.
pca_features = features.select("pca_features").rdd.map(lambda row : row[0])
mat = RowMatrix(pca_features)

# Once the RowMatrix is ready we can compute our Singular Value Decomposition
svd = computeSVD(mat,2,True)
svd.s
# DenseVector([9.491, 4.6253])
svd.U.rows.collect()
# [DenseVector([0.1129, -0.909]), DenseVector([0.463, 0.4055]), DenseVector([0.8792, -0.0968])]
svd.V
# DenseMatrix(2, 2, [-0.8025, -0.5967, -0.5967, 0.8025], 0)