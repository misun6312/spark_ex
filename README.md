# Spark Exercise (Jumble Solver)

## Installation

After clone this repository, to install all the required package you simply put: 
```
$ pip install -r requirements.txt 
```
This app will run on Spark. This code has been tested on Spark 2.3.2

## Usage

After you cloned the repository, you can run the App using :

```
$ spark-submit app.py
```

To change the input puzzle, put the input file name as INPUT_FILE variable in app.py. The input file should have first line as a first part of puzzle, which is a set of anagram and the corresponding circle indicator for the second part of puzzle. And the second line indicates the length of words for the sentence problem.

For example,

```
{'SHAST':'10011','DOORE':'11010','DITNIC':'111000','CATILI':'101001'}
[4, 8]
```

