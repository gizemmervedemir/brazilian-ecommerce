#ENVIRONMENT SET UP

#which env am I in conda
conda env list #opt + shift + e (satır bazlı çalıştırmak için)  alt+shift+e (windows için)

#Which packages are available in my env
conda list

#I am exporting the package environment needed for the New project from the base env to a file
conda env export > environment.yaml

#create a new env for the new project
conda create -n acm4762025_env

#activating the New project virtual env
conda activate acm4762025_env

#I accept base as origin and use the .yaml file for acm4762025_env
conda env create -f environment.yaml

#listing which packages are available
conda list

#updating all packages
#conda upgrade -all (Patlayabilir çünkü bazı paketler birbirleriyle uyumsuz olabilir)

#DATA ANALYSIS
#Starting data exploration

import numpy as np
import pandas as pd

#reading the sheet that contains raw data from the CSV file

df = pd.read_csv("/Users/gizemmervedemir/ACM476/data_ready_for_cs.csv")

#displaying the first 5 rows for verification
df.head

#step 1: performing descriptive statistics analyses

#Making a separate definition for numerical variables
num_var = [col for col in df.columns if df[col].dtype != '0']
num_var

#listing the basic descriptive functions
desc_agg = ['sum', 'mean' , 'std' , 'var' , 'min' , 'max']
desc_agg

#applying these functions to numerical values
desc_agg_dict = { col : desc_agg for col in df}
desc_agg_dict

desc_summ = df[num_var].agg(desc_agg_dict)

#printing desc_summ to examine each variable's sum, mean, standard deviation, min and max values
print(desc_summ)

#I want to convert it to a numpy array
df_desc_na = np.array(desc_summ)
df_desc_na

#to use df as a numpy array; for vector operations, etc.
df_na = np.array(df)
df_na

#Continuing Overview

import seaborn as sns

df.shape