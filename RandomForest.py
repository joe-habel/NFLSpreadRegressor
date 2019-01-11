"""
Here we're gonna use a random forest regressor to try to predict the score
differential. 
"""
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from numba import jit
import pickle

"""
Here's we're going to read the X,Y, and baseline data.
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
We'll convert those from pandas objects to numpy arrays. We don't have to with
statsmodels, since it's based on R's use of dataframes, but numpy does perform
much better than pandas. We'll save the names of the featuers in a list, so 
we can reference what is what.
"""

def to_arrays(X,Y,baseline):
    Y = np.array(Y)
    X_names = list(X.columns)
    X = np.array(X)
    baseline = np.array(baseline)
    return X,Y,X_names, baseline

"""
Here will be our error metrics for comparing our predictions to the baseline 
prediction performance.
"""

"""
Root Mean Square Error
"""
def get_rmse(Y,baseline):
    return np.sqrt(np.mean((Y-baseline)**2))


"""
Mean Absolute Error
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
    try:
        pred = pred.flatten()
    except:
        pass
    A = np.greater(pred,np.zeros_like(pred))
    B = np.greater_equal(act,pred)
    C = np.less(pred,np.zeros_like(pred))
    D = np.less_equal(act, pred)
    return np.mean(np.logical_or(np.logical_and(A,B) , 
                                 np.logical_and(C,D)))

"""
Data Augmentation is a useful technique that allows us to get more meaningful
data from what we already have.

An example I'm used to working with is performing Affine Transformations on
limited amounts of image data to get tons of more results.

Here what we're going to do is essntially mirror the game results around the 
teams. I.e. swap the home and away colums and negate the differnetial,
essentially doubling the size of our data.

We're jitting it here, just for that nice C speed array performance.
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
Here we're plotting our model
"""
def plot_model(mae,y_preds,y_vals,title,save=False):
    y_vals, y_preds = zip(*sorted(zip(y_vals,y_preds)))
    t = range(len(y_preds))
    plt.plot(t,y_vals,'go',label='Actual')
    plt.plot(t,y_preds,'r*', label='Predicted Values MAE=%02.2f'%mae)
    plt.xlabel('Ranked Game Number')
    plt.ylabel('Score Differential (Away - Home)')
    plt.title('%s'%title)
    plt.legend()
    if save:
        plt.savefig('Model Results/RF/%s.png'%title)
    plt.clf()

"""
Here we're splitting our training and test data
"""
def training_and_test(X,Y,test_size=0.2):
    X_train, X_test, Y_train, Y_test = train_test_split(X,Y,test_size=test_size, random_state=42)
    return X_train, X_test, Y_train, Y_test
"""
Here we're evaluating our model on our test set.
"""
def compare_errors(preds,baseline,Y,Y_test):
    errors = {}
    errors['spread_accuracy'] = spread_accuracy(Y_test,preds)
    errors['mae'] = get_mae(preds,Y_test)
    errors['rmse'] = get_rmse(preds,Y_test)
    errors['baseline_spread_accuracy'] = spread_accuracy(Y,baseline)
    errors['baseline_mae'] = get_mae(baseline,Y)
    errors['baseline_rmse'] = get_rmse(baseline,Y)
    return errors


"""
This shuffles each feature and observes how much each feautre being shuffled
affects the spread accuracy. This effect on the spread accuracy will be used
as our feature importances.
"""
def shuffle_importances(X_test,Y_test,model,names,acc):
    importances = {}
    for i in range(X_test.shape[1]):
        new_X = X_test.copy()
        np.random.shuffle(new_X[:,i])
        shuffled_preds = model.predict(new_X)
        shuffled_acc = spread_accuracy(Y_test,shuffled_preds)
        importances[names[i]] = abs(acc - shuffled_acc)/acc
    return importances


"""
This acts as our main for recursively finding the right features to fit our
Random Forest against.
"""
def generate_model(X_train,Y_train,X_test,Y_test,Y,baseline):
    base = RandomForestRegressor(n_estimators=1500,random_state=42,n_jobs=-1)
    base.fit(X_train,Y_train)
    
    preds=base.predict(X_test)
    errors = compare_errors(preds,baseline,Y,Y_test)
    print ("All Features, n=%s:"%X_train.shape[1])
    for name,error in errors.items():
        print ('...',name,error)
    
    best_acc = errors['spread_accuracy']
    #We're saving our best model here so we can later access it within our 
    #ipython notebook to access our SHAP plots along with the corresponding 
    #data 
    with open('Model Results/RF/rf.pkl', 'wb') as pkl_file:
        pickle.dump(base,pkl_file)
    train_data = ((X_train,X_test), (Y_train, Y_test))
    with open('Model Results/RF/data.pkl','wb') as data_pkl:
        pickle.dump(train_data,data_pkl)
    

    final_number_of_features = 45
    """
    Here we're getting the feature importances, and dropping the features which
    don't contribute up to the cumulatishve cutoff importance value for the features
    """
    important_train = X_train
    important_test = X_test
    while important_train.shape[1] > final_number_of_features:
        shuf_importances = shuffle_importances(important_test,Y_test,base,names,errors['spread_accuracy'])
        feature_importances = [(feature,importance) for feature, importance in shuf_importances.items()]
        feature_importances = sorted(feature_importances, key = lambda x: x[1], reverse=True)
        sorted_importances = [importance[1] for importance in feature_importances]
        cum_importances = np.cumsum(sorted_importances)
        max_val = cum_importances[-1]
        
        cutoff = np.where(cum_importances/max_val > 0.9)[0][0] + 1
        important_names = [feature[0] for feature in feature_importances[:cutoff]]
        important_indicies = [names.index(feature) for feature in important_names]
            
        important_train = important_train[:,important_indicies]
        important_test = important_test[:,important_indicies]
            
        base.fit(important_train,Y_train)
        preds = base.predict(important_test)
        errors = compare_errors(preds,baseline,Y,Y_test)
        print ("Dropped Features, n=%s:"%important_train.shape[1])
        for name,error in errors.items():
            print ('...',name,error)
        if errors['spread_accuracy'] > best_acc:
            #We're saving our best model here so we can later access it within our 
            #ipython notebook to access our SHAP plots along with the corresponding 
            #data
            best_acc = errors['spread_accuracy']
            with open('Model Results/RF/rf%s.pkl'%important_train.shape[1], 'wb') as pkl_file:
                pickle.dump(base,pkl_file)
            train_data = ((important_train,important_test),(Y_train,Y_test))
            with open('Model Results/RF/data%s.pkl'%X_train.shape[1],'wb') as data_pkl:
                pickle.dump(train_data,data_pkl)



X,Y,baseline = read_training(8,4)
X,Y,names,baseline = to_arrays(X,Y,baseline)
X,Y_aug = augment(X,Y) 
X_train, X_test, Y_train, Y_test = training_and_test(X,Y_aug,0.25)
generate_model(X_train,Y_train,X_test,Y_test,Y,baseline)
