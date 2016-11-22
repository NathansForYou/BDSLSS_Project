def mult_matr(mat1,mat2):
    #takes two rdds mat1 mxn and mat2 nxm and forms the matrix product
    #mat1*mat2 in mxm
    mat2 = rddTranspose(mat2)
    m = mat1.count()
    mat_cart = mat1.cartesian(mat2)
    mat_to_be_reshaped = mat_cart.map(lambda (arr1,arr2) : sum(map(lambda (x,y) : x*y, zip(arr1,arr2)))).zipWithIndex() #long rdd of m^2 entries where each entry is a scalar
    return mat_to_be_reshaped.groupBy(lambda (x,i): i/m).map(lambda row : list(row)).map(lambda (idx,row) : np.asarray([x for (x,i) in list(row)]))
    
