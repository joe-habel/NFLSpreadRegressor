"""
Here we're going to try to predict the score diffential using an ordinary least
squares model. You could use the scikit-learn for the OLS, but I like the 
summary that that statsmodels.api gives. It bears a close resemblacne to that
of R, which I find more informative than the scikit-learn outputs. 
"""
import pandas as pd
import statsmodels.api as sm
import matplotlib.pyplot as plt
import numpy as np
from collections import defaultdict
from sklearn.model_selection import train_test_split

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
Here we're splitting the data into training and test sets
"""

def training_and_test(X,Y,test_size=0.2):
    X_train, X_test, Y_train, Y_test = train_test_split(X,Y,test_size=test_size)
    return X_train, X_test, Y_train, Y_test


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
Here we'll plot the results.
"""
def plot_res(res,X):
    for name in X.columns:
        fig = plt.figure(figsize=(12,8))
        fig = sm.graphics.plot_regress_exog(res, name, fig=fig)
        fig.savefig('Model Results/OLS/%s.png'%name)

"""
Here we're going to construct an OLS model
"""
def make_model(X,Y,print_sum=False,title='OLS'):
    model = sm.OLS(Y,X)
    res = model.fit()
    if print_sum:
        print (res.summary())
        with open('Model Results/OLS/%s.txt'%title, 'w') as results:
            results.write(str(res.summary()))
    return res
"""
Here we're going to check for colinearity. We'll have to filter out any values
that would blow up a correlation matrix, which generally would be features that
are passed through that are constant, due to how a Pearson Coefficient is
calculated. This will filter out constant features, as well as all but a single
feature that appear to be collinear for each team. The eval_cutoff and evect_cutoff
defualt to a numpy is close to 0, which I believe has a tolerance of 1e-5. If you want
to be more forgiving you can specifiy a different tolerance level for each of the 
checks.
"""
def filter_collinear(X,eval_cutoff=None,evect_cutoff=None):
    name_index = list(X.columns)
    X = np.array(X)
    corr = np.corrcoef(X,rowvar=False)
    
    #Here we're going to filter out any columns that have nan values throughout
    #them
    consts =  np.argwhere(np.isnan(corr))
    if consts.size != 0:
        const_dict = defaultdict(int)
        for index in consts:
            const_dict[index[0]] += 1
        constant_cols = []
        for index, count in const_dict.items():
            if count == X.shape[1]:
                constant_cols.append(index)
        #We'll recalculate the correlation matrix so we don't have any nans,
        #after we drop those features from our X data.
        if bool(constant_cols):
            name_index = [val for i, val in enumerate(name_index) if i not in constant_cols]
            X = np.delete(X,constant_cols,axis=1)
            corr = np.corrcoef(X, rowvar=False)
    
    #Here we're going to go through the eigenvalues, and if the eigenvalue
    #is close to zero, either by numpy's standard or a specified value, 
    #we'll seperrate the data into two teams, and list each team's indicies
    w,v = np.linalg.eig(corr)
    collinear = defaultdict(list)
    for i,eigval in enumerate(w):
        if eval_cutoff is None:
            if np.isclose(eigval,0):
                for j, val in enumerate(v[:,i]):
                    if evect_cutoff is None:
                        if not np.isclose(val,0):
                            collinear[name_index[j].split(' ')[0]].append(j)
                    else:
                        if val > evect_cutoff:
                            collinear[name_index[j].split(' ')[0]].append(j)
        else:
            if eigval < eval_cutoff:
                for j, val in enumerate(v[:,i]):
                    if evect_cutoff is None:
                        if not np.isclose(val,0):
                            collinear[name_index[j].split(' ')[0]].append(j)
                    else:
                        if val > evect_cutoff:
                            collinear[name_index[j].split(' ')[0]].append(j)
    col_remove = []
    #We'll keep the first collinear element from each teams collinear features
    for team, indicies in collinear.items():
        col_remove.extend(indicies[1:])
    name_index = [val for i, val in enumerate(name_index) if i not in col_remove]
    X = np.delete(X,col_remove,axis=1)
    return pd.DataFrame(X,columns=name_index)
"""
Here we're going to filter out features based on their significance values to 
the OLS model, by their p values. I.e. a cutoff of 0.05 would only keep 
features with a pvalue less than five
"""
def filter_out(res,X,cutoff=0.2):
    for label,p in res.pvalues.iteritems():
        if p > cutoff:
            X = X.drop(label,axis=1)
    return X

"""
Here we'll make two models, one with all of the features, and one that filters
out features with a p value less than the cutoff. 
"""
def model_filter(filter_val,clip=None):
    X, Y = read_training()
    if bool(clip):
        Y = np.clip(Y,-clip,clip)
    res = make_model(X,Y,True,title='Simple OLS Clip')
    plot_res(res,X,Y,title='Simple OLS Clip',save=True)
    X = filter_out(res,X,filter_val)
    res = make_model(X,Y,True,title='Simple OLS Filtered Cutoff Clip')
    return res



"""
Here we'll wrap out a nice way to get both our baseline and errors of our 
OLS model.
"""
def compare_errors(preds,baseline,Y_test,Y):
    errors = {}
    errors['spread_accuracy'] = spread_accuracy(Y_test,preds)
    errors['mae'] = get_mae(preds,Y_test)
    errors['rmse'] = get_rmse(preds,Y_test)
    errors['baseline_spread_accuracy'] = spread_accuracy(Y,baseline)
    errors['baseline_mae'] = get_mae(baseline,Y)
    errors['baseline_rmse'] = get_rmse(baseline,Y)
    return errors

"""
Here we'll calculate the first linear model filtering out any collinear features 
"""
X,Y, baseline = read_training(8,4)
X = filter_collinear(X)
X_train, X_test, Y_train,Y_test = training_and_test(X,Y,0.1)
res = make_model(X_train,Y_train,False, 'full_OLS')
preds = res.predict(X_test)
errors = compare_errors(preds,baseline,Y_test,Y)
print ('Full OLS:')
for name,error in errors.items():
    print ('...',name,error)


"""
We'll calculate the second model by filtering out any values below our desired
significance level
"""
X = filter_out(res,X,0.05)
X_train, X_test, Y_train,Y_test = training_and_test(X,Y,0.1)
res = make_model(X_train,Y_train,False, 'filtered_OLS')
plot_res(res)
preds = res.predict(X_test)
errors = compare_errors(preds,baseline,Y_test,Y)
print ('Filtered OLS')
for name,error in errors.items():
    print ('...',name,error)


    
