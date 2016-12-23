'''
Purpose:
Project test points onto a pre-trained GMRA model.

Note: Must be run with python2.
'''

import mdtraj as md
import numpy as np
import gmra_no_spark as gm
import sys
import argparse
import pickle
from sklearn.cluster import KMeans
from scipy.cluster.vq import kmeans2
from time import time

t1=time()
argsp = argparse.ArgumentParser()
argsp.add_argument('--num_points', type=int, default=10000)
argsp.add_argument('--manifold_dim', type=int, default=10)
argsp.add_argument('--resolution', type=int, default=7)
argsp.add_argument('--file_num', type=int, default=0)
args = argsp.parse_args()

d = args.manifold_dim
res_num = args.resolution
num_points=args.num_points
file_num=args.file_num

G = gm.GMRA([],d,res_num)

f = open('bpti-prot-%d.b' % file_num,'rb')
f=pickle.load(f)
c_jks=f[1]
Phi_jks=f[2]
reps = [0 for i in xrange(len(f[1]))]
G.resolutions= [el if el[1]!=None else None for el in zip(reps,c_jks,Phi_jks)]
G.low_dim_reps = f[0]

topology_name = '/mddb2/md/bpti-prot/bpti-prot.pdb'
align_to = md.load('/mddb2/md/bpti-prot/bpti-prot-00.dcd',top=topology_name)
file_name = '/mddb2/md/bpti-prot/bpti-prot-0%d.dcd' % file_num
traj = md.load(file_name, top=topology_name)
traj.superpose(align_to)
traj = traj.xyz[80000:80000+num_points]
traj = np.reshape(traj,(num_points,892*3))

#G.proj_points(traj[0].reshape(-1,1),c_jks[0],Phi_jks[0])[0]

'''
projection method
'''
import operator
data_test=[]
for j in range(len(traj)):
    print('point',j)
    point=traj[j]
    res=[]
    for r in range(res_num):
        interval=2**r
        start=2**r-1
        end=2**r
        dist=[]
        for i in range(end-start):
            dist.append(np.linalg.norm(G.resolutions[i][1]-point))
        min_index, min_value = min(enumerate(dist), key=operator.itemgetter(1))
        ind=min_index
        #ind = dist.index(min(dist))
        ind = ind+start
        c=G.resolutions[ind][1]
        shape=c.shape
        c=np.reshape(c,(shape[0],))
        res.append(np.transpose(G.resolutions[ind][2]).dot(point-c))
    point=np.vstack(res)
    shape=point.shape
    point=np.reshape(point,(shape[0]*shape[1],))
    data_test.append(point)

data_test=np.vstack(data_test)

#optional: save file
#np.save('data_test%d_%d_%d.npy' % (file_num,d,res_num),data_test)
t2=time()

            
