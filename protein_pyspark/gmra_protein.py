# Currently being implemented on Spark 1.5.1
# GMRA research paper: https://arxiv.org/pdf/1105.4924.pdf
# GMRA research presentation: https://arxiv.org/pdf/1105.4924.pdf

# import mdtraj, use its api for loading in protein data
import mdtraj as md

# load in data
t = md.load('../../../md/bpti-prot/bpti-prot-00.dcd', top='../../../md/bpti-prot/bpti-prot.pdb')

# select the atoms in the topology file of type alpha
t.top.select_atom_indices('alpha')

# See the xyz coordinates of each atom in the file
t.xyz

# Find the current shape of the data
t.xyz.shape

# Get the first 1000 frames of xyz data
t_1k = t.xyz[0:1000]

# Convert into spark RDD to run PCA using ML
data = []
# try to find a way to optimize the vectorization
from pyspark.mllib.linalg import Vectors
for frame in t_1k:
  for atom in frame:
    data.append((Vectors.dense(atom),))

# Next, apply PCA with the following:
from pyspark.ml.feature import PCA
df = sqlContext.createDataFrame(data, ["features"])
pca = PCA(k=2, inputCol="features", outputCol="pca_features")
model = pca.fit(df)
model.transform(df).collect()[0].pca_features

#*********************************
# The following methods need to be implemented for RDDs:
# Specifically, input and output an RDD

#*********************************
# RDD addition

#*********************************
# RDD multiplication

#*********************************
# RDD Transpose

#*********************************
# RDD Inverse

#*********************************
# RDD Singular Value Decomposition

#*********************************
# RDD Principal Component Analysis

def estimateCovariance(df):
    """Compute the covariance matrix for a given dataframe.

    Note:
        The multi-dimensional covariance array should be calculated using outer products.  Don't
        forget to normalize the data by first subtracting the mean.

    Args:
        df:  A Spark dataframe with a column named 'features', which (column) consists of DenseVectors.

    Returns:
        np.ndarray: A multi-dimensional array where the number of rows and columns both equal the
            length of the arrays in the input dataframe.
    """
    m = df.select(df['features']).map(lambda x: x[0]).mean()
    dfZeroMean = df.select(df['features']).map(lambda x:   x[0]).map(lambda x: x-m)  # subtract the mean
    return dfZeroMean.map(lambda x: np.outer(x,x)).sum()/df.count()


from numpy.linalg import eigh

def pca(df, k=2):
    """Computes the top `k` principal components, corresponding scores, and all eigenvalues.

    Note: from pyspark.ml.feature import *
        All eigenvalues should be returned in sorted order (largest to smallest). `eigh` returns
        each eigenvectors as a column.  This function should also return eigenvectors as columns.

    Args:
        df: A Spark dataframe with a 'features' column, which (column) consists of DenseVectors.
        k (int): The number of principal components to return.

    Returns:
        tuple of (np.ndarray, RDD of np.ndarray, np.ndarray): A tuple of (eigenvectors, `RDD` of
        scores, eigenvalues).  Eigenvectors is a multi-dimensional array where the number of
        rows equals the length of the arrays in the input `RDD` and the number of columns equals
        `k`.  The `RDD` of scores has the same number of rows as `data` and consists of arrays
        of length `k`.  Eigenvalues is an array of length d (the number of features).
     """
    cov = estimateCovariance(df)
    col = cov.shape[1]
    eigVals, eigVecs = eigh(cov)
    inds = np.argsort(eigVals)
    eigVecs = eigVecs.T[inds[-1:-(col+1):-1]]
    components = eigVecs[0:k]
    eigVals = eigVals[inds[-1:-(col+1):-1]]  # sort eigenvals
    score = df.select(df['features']).map(lambda x: x[0]).map(lambda x: np.dot(x, components.T) )
    # Return the `k` principal components, `k` scores, and all eigenvalues
    return components.T, score, eigVals

 def varianceExplained(df, k=1):
     """Calculate the fraction of variance explained by the top `k` eigenvectors.

     Args:
         df: A Spark dataframe with a 'features' column, which (column) consists of DenseVectors.
         k: The number of principal components to consider.

     Returns:
         float: A number between 0 and 1 representing the percentage of variance explained
             by the top `k` eigenvectors.
     """
     components, scores, eigenvalues = pca(df, k)
     return sum(eigenvalues[0:k])/sum(eigenvalues)

    ### TEST

from pyspark.ml.feature import *
from pyspark.mllib.linalg import Vectors
data = [(Vectors.dense([0.0, 1.0, 0.0, 7.0, 0.0]),),
         (Vectors.dense([2.0, 0.0, 3.0, 4.0, 5.0]),),
         (Vectors.dense([4.0, 0.0, 0.0, 6.0, 7.0]),)]
df = sqlContext.createDataFrame(data,["features"])
pca_extracted = PCA(k=2, inputCol="features", outputCol="pca_features")
model = pca_extracted.fit(df)
model.transform(df).collect()


### Test with new model
comp, score, eigVals = pca(df)
score.collect()

# Please fill these sections with potential implementations you find online,
# after testing them on the damsl network to ensure they work properly.
