# Import pandas package to store and manipulate data
import pandas as pd
# Import csv package to convert pandas dataframe to csv file
import csv
# Import Counter package to do counting
from collections import Counter
# Import operator package to sort a dictoinary by its values
import operator
# Import NTLK package after the installation
from nltk import sent_tokenize,word_tokenize
from nltk.corpus import stopwords
from nltk.stem.snowball import SnowballStemmer
import nltk
from nltk.util import ngrams
#Import pymongo package and mongoclient
from pymongo import MongoClient
#import numpy
import bson

#connect to MongoDB database - Airline and collection - Rating
try:
    con = MongoClient ('localhost')
    db = con.Airline
    rating = db.Rating
    print "Connected successfully!!!"
except pymongo.errors.ConnectionFailure, e:
    print "Could not connect to MongoDB: %s" % e

#Print the total amount of ratings
print("\n\n")
results = rating.find().count()
print('The total number of entries are', results)

#Import data from mongo and create dataframe
cursor = rating.find({}) 
rating_fields = ['_id','authorcountry','recommended','airlinename','cabin','rating_overall','reviewcontent','route']
reviewdf = pd.DataFrame(list(cursor), columns = rating_fields)
print(reviewdf) #Print the dataframe

#Limit the dataframe to access 40000 records
reviewdf = reviewdf.iloc[:40000, :] 
print(reviewdf) 

#Creating dictionary for positive and negative sentiment
positive=[]
negative=[]
keys_to_ignore = ['Entry','Source','Defined']
with open('general_inquirer_dict.txt') as fin:
    reader = csv.DictReader(fin,delimiter='\t')
    for i,line in enumerate(reader):
        if line['Negativ']=='Negativ':
            if line['Entry'].find('#')==-1:
                negative.append(line['Entry'].lower())
            if line['Entry'].find('#')!=-1: #In General Inquirer, some words have multiple senses. Combine all tags for all senses.
                negative.append(line['Entry'].lower()[:line['Entry'].index('#')]) 
        if line['Positiv']=='Positiv':
            if line['Entry'].find('#')==-1:
                positive.append(line['Entry'].lower())
            if line['Entry'].find('#')!=-1: #In General Inquirer, some words have multiple senses. Combine all tags for all senses.
                positive.append(line['Entry'].lower()[:line['Entry'].index('#')])

fin.close()

pvocabulary=sorted(list(set(positive))) 
nvocabulary=sorted(list(set(negative))) 
print(pvocabulary, "\n\n")
print(nvocabulary)



# See data columns
reviewdf['poswdcnt']=0
reviewdf['negwdcnt']=0
reviewdf['lsentiment']=0
reviewdf_index=0

# Tokenize the words from the review documents to a word list
def getWordList(text,word_proc=lambda x:x):
    word_list=[]
    for sent in sent_tokenize(text):
        for word in word_tokenize(sent):
            word_list.append(word)
    return word_list

# If needed, a dictionary can be used for stemming
#stemmer = SnowballStemmer("english")

# The lists are used for storing the # of positive words, the # of negative words, 
# and the overall sentiment level for all the documents
# The length of each list is equal to the total number of review document
pcount_list=[]
ncount_list=[]
lsenti_list=[]

# Iterate all review documents
# For each word, look it up in the positive word list and the negative word list
# If found in any list, update the corresponding counts 
for text in reviewdf['reviewcontent']:
    vocabulary=getWordList(text,lambda x:x.lower())
    
    # Remove words with a length of 1
    #vocabulary=[word for word in vocabulary if len(word)>1] 
    
    # Remove stopwords
    #vocabulary=[word for word in vocabulary
    #            if not word in stopwords.words('english')]
    
    # Stem words
    #vocabulary=[stemmer.stem(word) for word in vocabulary]
                    
    pcount=0
    ncount=0
    for pword in pvocabulary:
        pcount += vocabulary.count(pword)
    for nword in nvocabulary:
        ncount += vocabulary.count(nword)
    
    pcount_list.append(pcount)
    ncount_list.append(ncount)
    lsenti_list.append(pcount-ncount)    
    
    #review.loc[review_index,'poswdcnt']=pcount
    #review.loc[review_index,'negwdcnt']=ncount
    #review.loc[review_index,'lsentiment']=pcount-ncount
    
    reviewdf_index += 1
    print(reviewdf_index)

# Storing word counts and overall sentiment into the dataframe
# So that we know the # of positive words, # of negative words, and sentiment
# for each review document

se=pd.Series(pcount_list)
reviewdf['poswdcnt']=se
se=pd.Series(ncount_list)
reviewdf['negwdcnt']=se
se=pd.Series(lsenti_list)
reviewdf['lsentiment']=se

#Export the final dataframe into CSV for importing in Neo4j
reviewdf.to_csv('lexiconout.csv')


# Run a logistic regression and machine learning prediction
train = reviewdf[:28000] #train data
test = reviewdf[28000:] #test data

train
test

import statsmodels.api as sm2
#fill nan values
train = train.fillna(0)
#for the independent variable
columns_touse= ['lsentiment']
# to take needed columns 
train[columns_touse]
# learning/training the model
logit=sm2.Logit(train['recommended'],train[columns_touse])
result=logit.fit() 
#prediction on test data
preds = result.predict(test[use_cols])
preds
#converting to the target variable (Threshold)
preds = (preds > 0.5).astype(int)
#building a confusion matrix to assess test data 
confusion_matrix(preds,test['recommended'])
#final summary of Logit Regression
print(result.summary())
         



