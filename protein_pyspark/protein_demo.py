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
from pyspark.mllib.linalg import Vectors
for frame in t_1k:
  for atom in frame:
    data.append((Vectors.dense(atom),))

# Next, apply PCA with the following:
from pyspark.ml.feature import PCA
df = sqlContext.createDataFrame(data, ["input_xyz"])
pca = PCA(k=2, inputCol="input_xyz", outputCol="pca_features")
model = pca.fit(df)
model.transform(df).collect()[0].pca_features
