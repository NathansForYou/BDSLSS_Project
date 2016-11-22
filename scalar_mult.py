def mult_by_sc(rdd,scalar):
    #multiplies by scalar
    return rdd.map(lambda arr : map(lambda arr_el : scalar*arr_el, arr))
