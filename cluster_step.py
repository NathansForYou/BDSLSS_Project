def cluster_step(rdd):
    #take an rdd with row-wise data and split it in two clusters with k-means
    #return the two rdd's wrt to the clusters
    kmeans_b = KMeans.train(rdd, 2, maxIterations=10, seed=50, initializationSteps=5)
    centers = kmeans_b.predict(rdd)
    to_split = rdd.zip(centers)
    #probably a better way to do this
    cluster_0 = to_split.filter(lambda (entry,cluster) : cluster == 0)
    cluster_1 = to_split.filter(lambda (entry,cluster) : cluster == 1)
    return cluster_0.map(lambda (entry,cluster) : entry), cluster_1.map(lambda (entry,cluster) : entry)
    
