
# ssh rnabi@damsl.cs.jhu.edu
# ssh mddb2
# cd /mddb2/bdt2016/spark1.5.1/bin/pyspark


import numpy as np
import mdtraj as md

from sklearn.cross_validation import train_test_split
from sklearn.neighbors import KNeighborsClassifier
from sklearn.linear_model import SGDClassifier, Perceptron
from sklearn.linear_model import PassiveAggressiveClassifier
from sklearn.linear_model import LogisticRegression
from sklearn import svm


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


# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Little toy example

# load in data and select a subset of it 
t = md.load('/mddb2/md/bpti-prot/bpti-prot-00.dcd', top='/mddb2/md/bpti-prot/bpti-prot.pdb')
t.top.select_atom_indices('alpha')
X = t.xyz[499:len(t):1000]
X = np.reshape(X, [100, 892*3])
X.shape

# read the labels into numpy 
labels = np.loadtxt("/mddb2/md/bpti_labels_ms.txt")[:, 1]
Y = labels[0:100]
Y.shape

classify(X, Y, classifier=classifiers[0], splitratio=0.3)


# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Real example
# cd /mddb2/bdt2016/gmra/gmra




