'''
Classification example on a small subset of the data.
Run with python3.

Simply copy and paste into terminal.
'''

import numpy as np
import gmra_no_spark as gm
import pickle
from sklearn.cluster import KMeans
from time import time

#parameter values:
d = 3
res_num = 7
num_points = 70000

'''
This is for formatting the data into numpy format.
'''
res=dict()
for j in range(10):
    file1 = pickle.load(open('bpti-prot-%d_3.b' % j,'rb'),encoding='latin1')
    reps = file1[0]
    for i in range(len(reps)):
        try:
            if reps[i].shape[0]<d:
                reps[i] = np.resize(reps[i],(d,reps[i].shape[1]))
            elif reps[i].shape[0]>d:
                reps[i]=reps[i][:d]
        except:
            continue
    shape=list(reps[0].shape)
    res[j] = np.reshape(reps[0],[1]+shape)
    matr = reps[1]
    for i in range(2,len(reps)):
        try:
            a=reps[i].shape
        except:
            continue
        if np.array_equal(matr,np.array(1)):
            matr=reps[i]
        else:
            try:
                matr=np.append(matr,reps[i],axis=1)
            except:
                print('matr',j,i)
            if matr.shape[1]==num_points:
                try:                
                    shape=[1]+list(matr.shape)
                    res[j]=np.append(res[j],np.reshape(matr,shape),axis=0)
                except:
                    print('res',i)
                matr=np.array(1)        


#printing resolution shapes:
for k in res:
    res[k]=res[k][:res_num]
    print(res[k].shape)

#flattening resolutions:
data=np.array(1)
for k in res:
    shape=list(res[k].shape)
    if np.array_equal(data,np.array(1)):
        data=res[k]
    else:
        data=np.append(data,res[k],axis=2)

data=np.reshape(data,(res_num*d,num_points*10)).transpose()    

#getting and formatting labels:
labels=np.loadtxt('/mddb2/md/bpti_labels_ms.txt')[:,1]
labels = np.array([1000*[x] for x in labels])
labels = np.reshape(labels, (-1,))
labels=labels[:1000000]
labels_tr=np.concatenate([labels[:70000],labels[99999:169999],labels[199999:269999],labels[299999:369999],labels[399999:469999],labels[499999:569999],labels[599999:669999],labels[699999:769999],labels[799999:869999],labels[899999:969999]])   
#labels_tr=np.concatenate([labels[:80000],labels[99999:179999],labels[199999:279999],labels[299999:379999],labels[399999:479999],labels[499999:579999],labels[599999:679999],labels[699999:779999],labels[799999:879999],labels[899999:979999]])   
labels_t=np.concatenate([labels[80000:90000],labels[180000:190000],labels[280000:290000],labels[380000:390000],labels[480000:490000],labels[580000:590000],labels[680000:690000],labels[780000:790000],labels[880000:890000],labels[980000:990000]])

#next is classification:

import mdtraj as md
from time import time

from sklearn.cross_validation import train_test_split
from sklearn.neighbors import KNeighborsClassifier
from sklearn.linear_model import SGDClassifier, Perceptron
from sklearn.linear_model import PassiveAggressiveClassifier
from sklearn.linear_model import LogisticRegression
from sklearn import svm
from sklearn import linear_model


def classify(X, Y, classifier, splitratio):
    name = classifier[0]
    clf = classifier[1]
    print("training %s" % name)
    X_train, X_test, y_train, y_test = \
                train_test_split(X, Y, test_size=splitratio)
    clf.fit(X_train, y_train)
    y_pred = clf.predict(X_test)
    accuracy = np.mean(y_pred == y_test)*100
    print(accuracy)


def classify2(X, Y, classifier, X_test,Y_test):
    name = classifier[0]
    clf = classifier[1]
    print("training %s" % name)
    clf.fit(X, Y)
    y_pred = clf.predict(X_test)
    accuracy = np.mean(y_pred == Y_test)*100
    print(accuracy)


# define different classifiers
classifiers = [
    ("KNneighbors", KNeighborsClassifier(n_neighbors=3)),
    ("SVM", svm.SVC()),
    ("SAG", LogisticRegression(solver='sag', tol=1e-1 )),
    ("SGD", SGDClassifier()),
    ("ASGD", SGDClassifier(average=True)),
    ("Perceptron", Perceptron()),
    ("Passive-Aggressive I", PassiveAggressiveClassifier(loss='hinge',
                                                         C=1.0)),
    ("Passive-Aggressive II", PassiveAggressiveClassifier(loss='squared_hinge',
                                                          C=1.0))
]

#running k-NN algorithm with 80:20 training:test split:
X=data[::50] #< - - Small subset of data
Y=labels_tr[::50]
t1=time()
classify(X,Y,("KNneighbors", KNeighborsClassifier(n_neighbors=7)),0.2)
t2=time()
print(t2-t1,'s')


