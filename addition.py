def add(rdd1,rdd2):
    #takes two rdds from a numpy_matrix the same shape and returns their sum
    if rdd1.count() != rdd2.count():
        raise Exception("Number of rows missmatch!")
    if len(rdd1.first()) != len(rdd2.first()):
        raise Exception("Number of cols missmatch!")
    mat3 = mat1.zip(mat2)
    return mat3.map(lambda (arr1,arr2) : arr1+arr2)
