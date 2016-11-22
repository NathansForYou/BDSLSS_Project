def rddTranspose(rdd):
    rddT1 = rdd.zipWithIndex()
            .flatMap(lambda (x,i): [(i,j,e) for (j,e) in enumerate(x)])
    rddT2 = rddT1.map(lambda (i,j,e): (j, (i,e)))
            .groupByKey().sortByKey()
    rddT3 = rddT2.map(lambda (i, x): sorted(list(x), 
                        cmp=lambda (i1,e1),(i2,e2) : cmp(i1, i2)))
    rddT4 = rddT3.map(lambda x: map(lambda (i, y): y , x))
    return rddT4.map(lambda x: np.asarray(x))
#Taken from http://www.data-intuitive.com/2015/01/transposing-a-spark-rdd/
