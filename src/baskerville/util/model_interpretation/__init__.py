# Sources:
# https://runawayhorse001.github.io/LearningApacheSpark/mc.html
# https://cloud.google.com/solutions/monte-carlo-methods-with-hadoop-spark
# https://towardsdatascience.com/an-implementation-and-explanation-of-the-random-forest-in-python-77bf308a9b76
# https://towardsdatascience.com/interpreting-random-forest-and-other-black-box-models-like-xgboost-80f9cc4a3c38
# https://towardsdatascience.com/how-to-visualize-a-decision-tree-from-a-random-forest-in-python-using-scikit-learn-38ad2d75f21c
# https://stackoverflow.com/questions/31782288/how-to-extract-rules-from-decision-tree-spark-mllib
# https://stackoverflow.com/questions/37129602/spark-mlib-decision-trees-probability-of-labels-by-features
# https://stackoverflow.com/questions/40558567/how-to-view-random-forest-statistics-in-spark-scala

# https://stats.stackexchange.com/questions/386558/feature-importance-in-isolation-forest
# https://discourse.pymc.io/t/priors-of-great-potential-how-you-can-add-fairness-constraints-to-models-using-priors-by-vincent-d-warmerdam-matthijs-brouns/5987/4

# call scala from python
# https://diogoalexandrefranco.github.io/scala-code-in-pyspark/
# https://aseigneurin.github.io/2016/09/01/spark-calling-scala-code-from-pyspark.html
# node = spark.sparkContext._jvm.org.apache.spark.ml.iforest.IFLeafNode
# model = spark.sparkContext._jvm.org.apache.spark.ml.iforest.IForestModel
# https://sourceforge.net/p/py4j/mailman/py4j-users/thread/CAP%3DZQSvuw9Hnk_DHUtHBU6UWUmJqJBSTB_5rZPdrsf%2BeZnG_zA%40mail.gmail.com/#msg33108805

# graph x and graph-frames:
# http://spark.apache.org/docs/latest/graphx-programming-guide.html#
# https://graphframes.github.io/graphframes/docs/_site/quick-start.html
# do this afterwards:
# https://stackoverflow.com/questions/49805284/how-to-get-node-information-on-spark-decision-tree-model

# https://stats.stackexchange.com/a/393437/140260
# I believe it was not implemented in scikit-learn because in contrast with
# Random Forest algorithm, Isolation Forest feature to split at each node is
# selected at random. So it is not possible to have a notion of feature
# importance similar to RF.
#
# Having said that, If you are very confident about the results of Isolation Forest
# classifier and you have a capacity to train another model then you could use the
# output of Isolation Forest i.e -1/1 values as target-class to train a Random
# Forest classifier.
# This will give you feature importance for detecting anomaly.

# Despite all this the iForest is interpretable if its underlying principle is
# taken advantage of: that outliers have short decision paths on average over
# all the trees in the iForest. The features that were cut on in these short
# paths must be more important than those cut on in long decision paths.
# So here's what you can do to get feature importances:

# https://github.com/scikit-learn/scikit-learn/issues/10642
# SHAP tree explainer
# https://towardsdatascience.com/explain-your-model-with-the-shap-values-bc36aac4de3d
# https://github.com/dataman-git/codes_for_articles/blob/master/Explain%20your%20model%20with%20the%20SHAP%20values%20for%20article.ipynb
# https://www.timlrx.com/2018/06/19/feature-selection-using-feature-importance-score-creating-a-pyspark-estimator/
