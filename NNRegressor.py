"""
Here we're gonna try to utilize an ANN for regression purposes.
"""
from keras.models import Sequential
from keras.layers import Dense, Dropout, Flatten
from keras.optimizers import Adam
from keras import backend as K
from keras import metrics
from theano.tensor import arctan, and_, or_
from numba import jit
import numpy as np
from sklearn.model_selection import train_test_split
import pandas as pd
import matplotlib.pyplot as plt


"""
Here we're defining metrics that Keras doesn't have under the hood.
"""

"""
Root Mean Square Error
"""
def rmse(y_true, y_pred):
    return K.sqrt(K.mean(K.square(y_pred - y_true), axis=-1))


"""
Mean Absolute Arctan Percent Error

We use to get an alternative to mean average percent error that doesn't force,
us to shift our outputs for 0 actual values.

And with small values we can interpret this the same we would MAPE
I.e. arctan(0/0) = 0
     arctan(10/0) =  pi/2 ~= 1.57
"""
def maape(y_true, y_pred):
    return K.mean(arctan(K.abs((y_pred-y_true)/y_true)))


"""
This calculates the percent of the time that the spread was correct.

For an explanation of what's going on here, the spread_accuracy lower in this
file goes into those details. 
"""
def spread_acc(y_true,y_pred):
    return K.mean(or_(and_(K.greater_equal(y_pred,0),K.greater_equal(y_true,y_pred)),
                      and_(K.less_equal(y_pred,0), K.less_equal(y_true,y_pred))))


"""
Here we're feeding the game data as an Nx2 array where the home and away values
are put up against each other. 

This is also probably way too deep for the data that we actually have to deal
with, and will probably lead to fast overtraining of the network. An ANN is 
probably not the best approach to this problem, as is, but I have the GPU power
so why not whip something up just to see how it goes.
"""
def create_model(input_dim,clip=False):
    model = Sequential()
    model.add(Dense(2*16,activation='relu',input_shape=(input_dim,2)))
    model.add(Dense(2*8,activation='relu'))
    model.add(Dropout(.3))
    model.add(Dense(2*8,activation='relu'))
    model.add(Dense(2*4,activation='relu'))
    model.add((Dropout(0.25)))
    model.add(Dense(2*2,activation='relu'))
    model.add(Dense(2,activation='relu'))
    model.add(Dense(1,activation='relu'))
    model.add(Flatten())
    model.add(Dense(15,activation='relu'))
    model.add(Dense(1,activation='linear'))
    
    if clip:
        adam = Adam(clipnorm=1.0)
    else:
        adam = Adam()
    model.compile(optimizer=adam, loss='mse', metrics=[rmse, maape, metrics.mae, spread_acc])
    return model
"""
Reading in the features, predictors, and baseline values.
"""
def read_training(player_memory=None,team_memory=10,playoffs=True):
   if player_memory is None:
       player_memory = 'All'
   data = pd.read_excel('Training/Player-%s,Team-%s,Playoffs-%s.xlsx'%(player_memory,team_memory,playoffs)) 
   data = data.fillna(0)
   Y = data['Score Differential']
   baseline = data['Vegas Baseline']
   X = data.drop('Score Differential',axis=1)
   X = X.drop('Vegas Baseline', axis=1)
   return X,Y,baseline
"""
Converting to np arrays so we can pass them into the model.
"""
def to_arrays(X,Y,baseline):
    Y = np.array(Y)
    X_names = list(X.columns)
    X = np.array(X)
    baseline = np.array(baseline)
    return X,Y,X_names, baseline

"""
Splits the training and test data.
"""
def training_and_test(X,Y,test_size=0.2):
    X_train, X_test, Y_train, Y_test = train_test_split(X,Y,test_size=test_size)
    return X_train, X_test, Y_train, Y_test
"""
Here we're augmenting our data by flipping the home and away teams, and negating
the differential to essentially double the size of our training set.
"""
@jit
def augment(X,Y):
    augmented_X = None
    augmented_Y = np.array([])
    mid = int(X.shape[1]/2)
    for i in range(X.shape[0]):
        aug_row = []
        for j in range(X.shape[1]):
            if j < mid:
                aug_row.append(X[i][mid+j])
            else:
                aug_row.append(X[i][j-mid])
        if augmented_X is None:
            augmented_X = np.array(aug_row)
        else:
            augmented_X = np.vstack((augmented_X,np.array(aug_row)))
        augmented_Y = np.append(augmented_Y,-Y[i])
    X = np.vstack((X,augmented_X))
    Y = np.append(Y,augmented_Y)
    return X,Y
"""
Here we're changing the 1D shape that we were using for other models into a 
2D shape that pairs up the home and away values.
"""
@jit
def flat_to_2d(X):
    X2d = []
    offset = int(X.shape[1]/2)
    for i in range(X.shape[0]):
        row = []
        for j in range(int(X.shape[1]/2)):
            row.append([X[i][j],X[i][j+offset]])
        X2d.append(row)
    return np.asarray(X2d)

"""
Calculating our metrics to get the baseline errors.
"""

"""
Root Mean Square Error
"""
def get_rmse(Y,baseline):
    return np.sqrt(np.mean((Y-baseline)**2))
"""
Mean Absolute Arctan Percent Error
"""
def get_maape(Y,baseline):
    return np.mean(np.arctan(np.abs((Y-baseline)/Y)))
"""
Mean Average Error
"""
def get_mae(Y,baseline):
    return np.mean(np.abs(Y-baseline))
"""
Spread accuracy ends up being a slightly convoluted computation because of the 
a spread is always defined on the favored team. In our case we had converted those
spread lines to work against the home team no matter what, but that also means
we had to negate some of them. So positive spreads imply the away team as a
favorite, while negative spreads imply the home team. So if the spread was right
and favored the home team, the spread will be negative and the the scoring
differential will have to be "less than that negative spread. So that if the home team
is favored, the home team has to score more points. While if the spread is positive
the scoring differential has to greater than the positive spread. And here we're
going to get the total number that meet either of those two requirements.
"""
def spread_accuracy(act,pred):
    pred = pred.flatten()
    A = np.greater(pred,np.zeros_like(pred))
    B = np.greater_equal(act,pred)
    C = np.less(pred,np.zeros_like(pred))
    D = np.less_equal(act, pred)
    return np.mean(np.logical_or(np.logical_and(A,B) , 
                                 np.logical_and(C,D)))

"""
Here we're plotting our error metrics against the number of training epochs.
"""
def make_plot(history,plot,epochs,baseline):
    plt.plot(history.history[plot], label='train')
    plt.plot(history.history['val_%s'%plot], label='test')
    if plot == 'mean_absolute_error':
        plot = 'mae'
    plt.hlines(baseline,0,epochs,colors='r',label='Vegas Baseline')
    plt.xlabel('Epcoh')
    plt.ylabel(plot.upper())
    plt.legend()
    plt.savefig('Model Results/NN/%s.png'%plot)
    plt.clf()

"""
This acts as our main
"""
def evaluate_model(batch_size= 40, epochs = 250,player_memory=None,team_memory=10,playoffs=True):
    X,Y,baseline = read_training(8,4)
    X,Y,names,baseline = to_arrays(X,Y,baseline)
    baseline_rmse = get_rmse(Y,baseline)
    baseline_maape = get_maape(Y,baseline)
    baseline_mae = get_mae(Y,baseline)
    baseline_accuracy = spread_accuracy(Y,baseline)
    X,Y_aug = augment(X,Y)
    #Y = np.clip(Y, -15,15)
    X = flat_to_2d(X)
    model = create_model(X.shape[1])
    X_train, X_test, Y_train, Y_test = training_and_test(X,Y_aug,.35)
    model.summary()
    history = model.fit(X_train,Y_train, batch_size, epochs, validation_data=(X_test,Y_test))
    make_plot(history,'mean_absolute_error',epochs,baseline_mae)
    make_plot(history,'rmse',epochs,baseline_rmse)
    make_plot(history,'maape',epochs,baseline_maape)
    make_plot(history,'spread_acc',epochs,baseline_accuracy)
    preds = model.predict(X_test)
    print (baseline_accuracy)
    print (spread_accuracy(Y_test,preds))


evaluate_model()