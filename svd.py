# cd spark1.5.1
# bin/pyspark

# Taken from: http://stackoverflow.com/questions/33428589/pyspark-and-pca-how-can-i-extract-the-eigenvectors-of-this-pca-how-can-i-calcu/33500704#33500704

from pyspark.mllib.common import callMLlibFunc, JavaModelWrapper
from pyspark.mllib.linalg.distributed import RowMatrix

# Define an SVD object
class SVD(JavaModelWrapper):
    """Wrapper around the SVD scala case class"""
    @property
    def U(self):
        """ Returns a RowMatrix whose columns are the left singular vectors of the SVD if computeU was set to be True."""
        u = self.call("U")
        if u is not None:
            return RowMatrix(u)
    @property
    def s(self):
        """Returns a DenseVector with singular values in descending order."""
        return self.call("s")
    @property
    def V(self):
        """ Returns a DenseMatrix whose columns are the right singular vectors of the SVD."""
        return self.call("V")


# Define the computeSVD method using the Java Wrapper
def computeSVD(row_matrix, k, computeU=False, rCond=1e-9):
    """
    Computes the singular value decomposition of the RowMatrix.
    The given row matrix A of dimension (m X n) is decomposed into U * s * V'T where
    * s: DenseVector consisting of square root of the eigenvalues (singular values) in descending order.
    * U: (m X k) (left singular vectors) is a RowMatrix whose columns are the eigenvectors of (A X A')
    * v: (n X k) (right singular vectors) is a Matrix whose columns are the eigenvectors of (A' X A)
    :param k: number of singular values to keep. We might return less than k if there are numerically zero singular values.
    :param computeU: Whether of not to compute U. If set to be True, then U is computed by A * V * sigma^-1
    :param rCond: the reciprocal condition number. All singular values smaller than rCond * sigma(0) are treated as zero, where sigma(0) is the largest singular value.
    :returns: SVD object
    """
    java_model = row_matrix._java_matrix_wrapper.call("computeSVD", int(k), computeU, float(rCond))
    return SVD(java_model)



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