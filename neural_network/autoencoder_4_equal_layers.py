# ssh into damsl:
# ssh name@damsl.cs.jhu.edu
# ssh mddb2

# To run tensorflow on damsl, enter the tflow docker:
# docker exec -it tflow bash
# This has access to the protein data and our bdt2016 folder
# To get access, email Ben Ring to be added as a docker admin

# import mdtraj, use its api for loading in protein data
import mdtraj as md
import tensorflow as tf
import numpy as np


# load in data
t = md.load('/mbbd2/md/bpti-prot/bpti-prot-00.dcd', top='/mbbd2/md/bpti-prot/bpti-prot.pdb')

# Get the first 1000 frames of xyz data
# Experimenting with number of test and train frames
t_train_input = t.xyz[0:20000]
t_test_input = t.xyz[20000:25000]


# other potential flattening, SIGNIFICANTLY more efficient
t_train = np.reshape(t_train_input, [20000, 892 * 3])
t_test = np.reshape(t_test_input, [5000, 892 * 3])

# Parameters
# edit parameters to tweak encoder
learning_rate = 0.01
training_epochs = 5
batch_size = 100
display_step = 1


# Network Parameters
n_hidden_1 = 2676 # 1st layer num features
n_hidden_2 = 2676 # 2nd layer num features
n_hidden_3 = 2676 # 3rd layer num features
n_hidden_4 = 3 # final hidden layer
n_input = 2676 # protein data input (img shape: 892*3)

X = tf.placeholder("float", [None, n_input])

weights = {
    'encoder_h1': tf.Variable(tf.random_normal([n_input, n_hidden_1])),
    'encoder_h2': tf.Variable(tf.random_normal([n_hidden_1, n_hidden_2])),
    'encoder_h3': tf.Variable(tf.random_normal([n_hidden_2, n_hidden_3])),
    'encoder_h4': tf.Variable(tf.random_normal([n_hidden_3, n_hidden_4])),
    'decoder_h1': tf.Variable(tf.random_normal([n_hidden_4, n_hidden_3])),
    'decoder_h2': tf.Variable(tf.random_normal([n_hidden_3, n_hidden_2])),
    'decoder_h3': tf.Variable(tf.random_normal([n_hidden_2, n_hidden_1])),
    'decoder_h4': tf.Variable(tf.random_normal([n_hidden_1, n_input])),
}
biases = {
    'encoder_b1': tf.Variable(tf.random_normal([n_hidden_1])),
    'encoder_b2': tf.Variable(tf.random_normal([n_hidden_2])),
    'encoder_b3': tf.Variable(tf.random_normal([n_hidden_3])),
    'encoder_b4': tf.Variable(tf.random_normal([n_hidden_4])),
    'decoder_b1': tf.Variable(tf.random_normal([n_hidden_3])),
    'decoder_b2': tf.Variable(tf.random_normal([n_hidden_2])),
    'decoder_b3': tf.Variable(tf.random_normal([n_hidden_1])),
    'decoder_b4': tf.Variable(tf.random_normal([n_input])),
}


# Building the encoder using rectified linear units for activation
def encoder(x):
    # Encoder Hidden layer with relu activation #1
    layer_1 = tf.nn.relu(tf.add(tf.matmul(x, weights['encoder_h1']),
                                   biases['encoder_b1']))
    # Decoder Hidden layer with relu activation #2
    layer_2 = tf.nn.relu(tf.add(tf.matmul(layer_1, weights['encoder_h2']),
                                   biases['encoder_b2']))
    # Decoder Hidden layer with relu activation #2
    layer_3 = tf.nn.relu(tf.add(tf.matmul(layer_2, weights['encoder_h3']),
                                   biases['encoder_b3']))
    # Decoder Hidden layer with relu activation #2
    layer_4 = tf.nn.relu(tf.add(tf.matmul(layer_3, weights['encoder_h4']),
                                   biases['encoder_b4']))
    return layer_4

# Building the decoder
def decoder(x):
    # Encoder Hidden layer with relu activation #1
    layer_1 = tf.nn.relu(tf.add(tf.matmul(x, weights['decoder_h1']),
                                   biases['decoder_b1']))
    # Decoder Hidden layer with relu activation #2
    layer_2 = tf.nn.relu(tf.add(tf.matmul(layer_1, weights['decoder_h2']),
                                   biases['decoder_b2']))
    # Decoder Hidden layer with relu activation #2
    layer_3 = tf.nn.relu(tf.add(tf.matmul(layer_2, weights['decoder_h3']),
                                   biases['decoder_b3']))
    # Decoder Hidden layer with relu activation #2
    layer_4 = tf.nn.relu(tf.add(tf.matmul(layer_3, weights['decoder_h3']),
                                   biases['decoder_b3']))
    return layer_4


# Construct model
encoder_op = encoder(X)
decoder_op = decoder(encoder_op)

# Prediction
y_pred = decoder_op
# Targets (Labels) are the input data.
y_true = X

# Define loss and optimizer, minimize the squared error
# Rewrite loss for planned algorithm
cost = tf.reduce_mean(tf.pow(y_true - y_pred, 2))
optimizer = tf.train.RMSPropOptimizer(learning_rate).minimize(cost)

# Initializing the variables
init = tf.initialize_all_variables()

# Launch the graph
with tf.Session() as sess:
    sess.run(init)
    total_batch = int(t_train.shape[0]/batch_size)
    # Training cycle
    for epoch in range(training_epochs):
        # Loop over all batches
        batch_start = 0
        for i in range(total_batch):
            batch_end = batch_start + batch_size
            #print "Batch start: " + str(batch_start)
            #print "Batch end: " + str(batch_end)
            batch_xs = t_train[batch_start:batch_end][:]
            batch_ys = t_train[batch_start:batch_end][:]
            # Run optimization op (backprop) and cost op (to get loss value)
            _, c = sess.run([optimizer, cost], feed_dict={X: batch_xs})
            batch_start += batch_size
        # Display logs per epoch step
        if epoch % display_step == 0:
            print("Epoch:", '%04d' % (epoch+1),
                  "cost=", "{:.9f}".format(c))
    print("Optimization Finished!")
    # Applying encode and decode over test set
    encode_decode = sess.run(
        y_pred, feed_dict={X: t_test})

# Now perform comparisons against initial test data and decoded,
# evaluate accuracy by average squared difference between them?
# Look into potentially accuracy metrics more!
t_test[0]
encode_decode[0]

# Basic squared difference metric
total_cost = np.sum((t_test - encode_decode)**2)
average_cost = total_cost / (t_test.shape[0] * t_test.shape[1])
