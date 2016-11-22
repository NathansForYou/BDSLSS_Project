def mult_by_vec(vec, mat):
#takes a d-dim array (vec) and an rdd (mat lxd) where each entry is a d-dim array
#returns a l-dim array
    if len(vec) != len(mat.first()):
        raise Exception("Dimension missmatch")
    mat_zipped = mat.map(lambda arr : zip(arr,v))
    return mat_zipped.map(lambda arr : map(lambda (x,y) : x*y, arr)).map(sum).collect()
