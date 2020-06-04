[comment]: # Copyright (c) 2020, eQualit.ie inc.
[comment]: # All rights reserved.

[comment]: # This source code is licensed under the BSD-style license found in the 
[comment]: # LICENSE file in the root directory of this source tree.


Baskerville Use Cases
=============

An outline of the different possible use cases and pipelines for Baskerville. 
This list is intended to be comprehensive, covering _all possible_ use cases, 
from which the directions to pursue can be subsequently selected.


# Outline of Use Cases

0. [Processing](#processing)
   - Processing real-time incoming logs to calculate request sets and features
   - Processing historic logs from elasticsearch to calculate request sets and features
   - Processing historic logs saved locally to calculate request sets and features
   - Cross-referencing logs with IPs identified in the MISP database
   - Cross-referencing logs with IPs previously predicted as malicious
   - Predicting whether request sets are malicious or benign based on features

1. [Monitoring](#monitoring)
   - Alterting when anomalous traffic is identified
   - Banning when anomalous traffic is identified
   - Publishing real-time statistics on traffic to a dashboard
   - Publishing real-time statistics on traffic to the eq website 
   - Gathering long-term statistics on Deflect traffic
    
2. [Investigations](#investigations)
   - Visualising traffic during past attacks in feature space  
   - Comparing IP behaviour between attacks in feature space to link attacks / attack tools
   - Clustering IPs into 'botnets' of similar behaviour to link attacks / attack tools
   - Sharing attack signatures with other interested groups
   
3. [Development](#development)
   - Syncing between the MISP database and baskerville database
   - Training a model based on historic request set labels to predict anomalies
   - Experimenting with adding / removing features to improve model accuracy
   - Experimenting to see how model accuracy improves with training data size
   - Experimenting with time bucket lengths and request set updating methods
 
   
# Processing

* **Initialize:**
   - fresh_slate: Wipe dataframes in memory, ready to start a new runtime, OR
   - load_cache: Query baskerville database for request sets meeting cache criteria, 
   and load them into dataframe.

* **Create Runtime:**
   - create_runtime: Add an entry to the runtimes table in the database, noting that 
   Baskerville has been run, and how it is being run.

* **Get Data:**
   - get_df: Load dataframe of log data from elasticsearch / kafka / raw file.

* **Preprocessing:**
   - handle_missing_columns: Add place holders for fields that are missing, 
   unless the host/IP is missing, in which case the log is dropped.
   - rename_columns: Rename fields with invalid spark names 
   (e.g. 'geoip.location.lat' -> 'geoip_location_lat').
   - filter_columns: Select just the group_by_cols and active_cols from the dataframe.
   - handle_missing_values: Insert null where values are missing in the dataframe.
   - add_calc_columns: Calculate additional fields needed to compute features later on, 
   and add to dataframe.

* **IP-host grouping:**
   - group_by_attrs: Group the dataframe of logs by host-ip.

* **Feature caculation:**
   - add_post_groupby_columns: Calculate additional fields needed to compute features / 
   perform other operations later on, and add to dataframe.
   - feature_extraction: For each feature compute the feature value and add it as 
   a column in the dataframe.
   - update_features: Use feature update methods (e.g. weighted average) 
   to compute new request set features from old and current feature values.

* **Label or predict:**
   - label_or_predict: Run cross-referencing to label request sets, 
   or predict request set label using ML model.
   - cross_reference: Search for the request set IP in the attributes table of the 
   Baskerville database, and label the request set as malicious if it is found.

* **Save:**
   - save: Save request set columns of dataframe to request sets table, 
   and save subset columns of dataframe to subsets table.
   - drop_expired: Check when each request set in the cache was last updated, 
   and remove it if that time is longer ago than the cache expiry time.

* **Finish up:**
   - unpersist: unpersist all spark dataframes so they do not take up space in memory.

# Monitoring

* **Metrics exporter**:

* **Grafana dashboard**:

# Investigations

* **Offline tools - visualisation**: Inspect incidents by visualising features
using various plotting techniques, and colouring via attack id, runtime, 
prediction, label, or cluster id.

* **Offline tools - clustering**: For a set of request sets from the baskerville
database, perform clustering to identify similar groups of IPs in feature space.

# Development

* **Offline tools - MISP sync**: Scrape MISP data, save events to Baskerville
database's attacks table, and if the attack source was Deflect, save the IPs
involved to the linked attributes table. 

* **Offline tools - labelling**: Label existing request sets in the Baskerville
database as malicious (-1) or benign (+1) based on e.g. cross-referencing with MISP. 

* **Offline tools - training**: Specify training and testing data in Baskerville
database. Train model on the former, assess its performance using the latter.

* **Offline tools - visualisation**: Feature importances / feature correlation
plots for feature selection. Incident-model comparison plots for model evaluation.

* **Experiments - attack identification**: Plot timeseries of features 
before-during-after an attack, to determine if there is a signal in the feature, 
and therefore if the feature may be useful for identifying malicious behaviour.

* **Experiments - model accuracy**: Plot the increase in model accuracy as a function
of the training data size, to determine a desirable quantity of training data to
process. 

* **Experiments - time bucket length**: Plot the feature values as a function of
the request set length, and the processing time is subsequently increased from the
same start point, to assess the minimum length of time need to approximate an IP's
request behaviour.

* **Experiments - time bucket updating**: Process the same period of logs using
different time bucket lengths, and plot the feature values as a function
of the bucket length, to assess the consistency of the feature updating
methods.

