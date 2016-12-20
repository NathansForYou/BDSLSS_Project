def center_data_matr(rdd):
    #take an rdd containing points row-wise
    #return a meme-centered rdd which is tranposed
    rdd_t = rddTranspose(rdd)
    meme_zipped = rdd_t.zip(rdd_t.map(np.mean))
    meme_centered = meme_zipped.map(lambda (row,m): np.asarray(map(lambda row_el: row_el - m, row)))
    return rddTranspose(meme_centered) #maybe we should transpose it back?

'''def center_col_matr(rdd_t):
    meme_zipped = rdd_t.zip(rdd_t.map(np.mean))
    meme_centered = meme_zipped.map(lambda (row,m): np.asarray(map(lambda row_el: row_el - m, row)))
    return meme_centered
'''

def meme_of_columns(rdd):
    return rdd.map(np.mean)

def substr_vec(rdd,vec):
    #takes an rdd with each entry a single float
    #substracts the vector from all of the columns of the given rdd
    rdd_t = rdd
    meme_zipped = rdd_t.zip(vec)
    meme_centered = meme_zipped.map(lambda (row,m): np.asarray(map(lambda row_el: row_el - m, row)))
    return meme_centered


