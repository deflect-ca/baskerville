# Baskerville Kubernetes deployment

## Nodepools
We assume that you have two nodepools: `workers` and `postgres`

## Kafka TLS keys 
* create truststore
```commandline
mkdir kafka
cd kafka
mkdir keys
git clone https://github.com/confluentinc/confluent-platform-security-tools
cd /confluent-platform-security-tools
export COUNTRY=..
export STATE=..
export ORGANIZATION_UNIT=...
export CITY=...
export PASSWORD=password_kafka
bash ./kafka-generate-ssl-automatic.sh
rm -r keystore 
cp truststore ../keys/truststore
echo $PASSWORD >~/keys/creds
```

* generate KAFKA keys 
Answer no to the first question(you do not want to recreate the truststore). 
Type './truststore/kafka.truststore.jks' when prompted for truststore.
Type `./truststore/kafka.truststore.jks` when prompted for truststore private key.
You can also use a new client password. 

```
bash ./kafka-generate-ssl.sh
mv keystore/kafka.keystore.jks ../keys/kafka-0.keystore.jks

bash ./kafka-generate-ssl.sh
mv keystore/kafka.keystore.jks ../keys/kafka-1.keystore.jks

bash ./kafka-generate-ssl.sh
mv keystore/kafka.keystore.jks ../keys/kafka-2.keystore.jks

cd ..
```

* create Kafka secret:
```commandline
kubectl create secret generic kafka-jks --from-file=./truststore/kafka.truststore.jks --from-file=./kafka-0.keystore.jks --from-file=./kafka-1.keystore.jks --from-file=./kafka-2.keystore.jks
```
# Kafka

Deploy Kafka from https://github.com/bitnami/charts.git

* Modify if needed `deployment/kafka/values-kafka.yaml`
```yaml
auth:
  jksPassword: your_jks_password

nodeSelector:
  nodepool: workers
```

* deploy kafka
```commandline
helm repo add bitnami https://charts.bitnami.com/bitnami
helm install kafka -f deployment/kafka/values-kafka.yaml bitnami/kafka
```

* follow the displayed instruction to get kafka connection string:
```commandline
Kafka brokers domain: You can get the external node IP from the Kafka configuration file with the following commands (Check the EXTERNAL listener)

        1. Obtain the pod name:

        kubectl get pods --namespace default -l "app.kubernetes.io/name=kafka,app.kubernetes.io/instance=kafka,app.kubernetes.io/component=kafka"

        2. Obtain pod configuration:

        kubectl exec -it KAFKA_POD -- cat /opt/bitnami/kafka/config/server.properties | grep advertised.listeners

    Kafka brokers port: You will have a different node port for each Kafka broker. You can get the list of configured node ports using the command below:

        echo "$(kubectl get svc --namespace default -l "app.kubernetes.io/name=kafka,app.kubernetes.io/instance=kafka,app.kubernetes.io/component=kafka,pod" -o jsonpath='{.items[*].spec.ports[0].nodePort}' | tr ' ' '\n')"
```
* construct external kafka connection string. It should be something like this: `ip_broker1:30001,ip_broker2:30002,ip_broker3:30003`
```commandline
kubectl exec -it kafka-0 -- cat /opt/bitnami/kafka/config/server.properties | grep advertised.listeners
kubectl exec -it kafka-1 -- cat /opt/bitnami/kafka/config/server.properties | grep advertised.listeners
kubectl exec -it kafka-2 -- cat /opt/bitnami/kafka/config/server.properties | grep advertised.listeners
```
## Kafka ACL(Access Control List)
ACL enables to control external user access to Kafka topics. An external user should have access only to the `features` topic in Clearinghouse Kafka.
* Uncomment ACL from 'deployment/kafka/values-kafka.yaml'
```yaml
extraEnvVars:
 - name: KAFKA_CFG_AUTHORIZER_CLASS_NAME
   value: "kafka.security.authorizer.AclAuthorizer"
```
* Enable ACL
```commandline
helm upgrade --namespace default kafka bitnami/kafka -f ./deployment/kafka/values-kafka.yaml

```


## Redis

``` 
* modify nodeSelector(s) if needed `deployment/redis/values-redis.yaml'

* deploy redis
```commandline
helm install redis -f deployment/redis/values-redis.yaml bitnami/redis
```

## Baskerville Configuration
Baskerville configuration files are supposed to be stored in a separate github repository.
This repository should contain four files:
* preprocessing.yaml
* predicting.yaml
* postprocessing.yaml
* training.yaml

The templates for the above files are in the 'conf' directory.
In order to access that repo from the cluster you need to create a new SSH key and add the public key to that github 
repository. 

## SSH key
* create a new ssh key and add it to your github configuration repository
* add `github.com` key to `known_hosts` file:
```commandline
ssh-keyscan github.com >> /ssh/known_hosts
```
* create a kubernetes ssh secret 
```commandline
kubectl create secret generic ssh-secrets --from-file=id_rsa=./ssh/id_rsa  --from-file=known_hosts=./ssh/known_hosts
```

## Baskerville secrets
* edit `baskerville_secrets.yaml with your credentials and passwords
```
* create secret
```
k apply -f deployment/baskerville_secrets.yaml
```

## Spark binding
Create cluster role binding
```
kubectl create clusterrolebinding default --clusterrole=edit --serviceaccount=default:default --namespace=default
```

## ARGO
* install ArgoProj
```commandline
kubectl create namespace argo
kubectl apply -n argo -f https://raw.githubusercontent.com/argoproj/argo/v3.0.0-rc3/manifests/install.yaml
```
* run port forwarding to access Argo UI
```commandline
kubectl -n argo port-forward deployment/argo-server 2746:2746
```
* open `localhost:2746` in a browser and make sure Argo UI is up and running.

## Argo workflows
Create four argo workflow templates from each file in `deployment/argo'. 
* open 'localhost:2746'
* click on `Wofkflow Templates` on the left vertical bar
* copy paste the content of the file
* save the template

## Postgres
* deploy postgres pod:
```
kubectl apply -f deployment/postgres/postgres.yaml
```

* deploy load balancer:
```
kubectl apply -f deployment/postgres/postgres_lb.yaml
```

## Grafana
* set your postgres password in the datasource `deployment/grafana/datasources/postgres.yaml`
```yaml
datasources:
  - name: PostgreSQL
      secureJsonData:
        password: ...
```

* create secret:
```commandline
kubectl create secret generic datasource-secret --from-file=deployment/grafana/datasources/postgres.yaml
```
* create dashboard config maps:
```commandline
kubectl create configmap dashboard-attack --from-file=deployment/grafana/dashboards/Attack.json
kubectl create configmap dashboard-trafficlight --from-file=deployment/grafana/dashboards/TrafficLight.json
```

* deploy grafana:
```commandline
helm install grafana -f deployment/grafana/values-grafana.yaml bitnami/grafana
```

## Baskerville images
* Spark image (https://levelup.gitconnected.com/spark-on-kubernetes-3d822969f85b)
```commandline
wget https://archive.apache.org/dist/spark/spark-2.4.6/spark-2.4.6-bin-hadoop2.7.tgz
mkdir spark
mv spark-2.4.6-bin-hadoop2.7.tgz spark
tar -xvzf spark-2.4.6-bin-hadoop2.7.tgz
vi ~/.profile
export SPARK_HOME=/root/spark/spark-2.4.6-bin-hadoop2.7
alias spark-shell=”$SPARK_HOME/bin/spark-shell”

$SPARK_HOME/bin/docker-image-tool.sh -r baskerville -t spark2.4.6 -p $SPARK_HOME/kubernetes/dockerfiles/spark/bindings/python/Dockerfile build

docker tag baskerville/spark-py:v2.4.6 equalitie/baskerville:spark246
```
* build Baskerville image
```commandline
docker build -t equalitie/baskerville:worker dockerfiles/worker/
```
* build the latest Baskervilli image
```commandline
docker build -t equalitie/baskerville:latest .
docker push equalitie/baskerville:latest
```
## Run Baskerville
* Add a Kafka output to your logstash instance. You should use the external kafka connection url you created. 
See an example in [Baskerville Client repository](https://github.com/equalitie/baskerville_client)
* Launch `Preprocessing`, `Predicting`, `Postprocessing` workflows.
* Launch `Training' workflow to retrain the model.

## Jupyter Notebook
* create `spark` namespace `kubectl create namespace spark`
* create service account:
```commandline
kubectl create serviceaccount spark -n spark
```
* create role binding
```commandline
kubectl create clusterrolebinding spark-role --clusterrole=edit --serviceaccount=spark:spark --namespace=spark
```
* build notebook image
```commandline
docker build ./deployment/notebook -t equalitie/baskerville:notebook
```

* edit `/deployment/notebook/spark_secrets.yaml with your credentials and passwords

* create spark secrets
```
k apply -f spark_secrets.yaml 
```
* deploy notebook PVC
```commandline
kubectl apply -f ./deployment/notebook/notebook-pvc.yaml
```
* deploy notebook
```commandline
kubectl apply -f ./deployment/notebook/notebook.yaml
```

* create notebook port forwarding
```commandline
kubectl port-forward -n spark deployment.apps/notebook-deployment 8888:8888
````

* open `localhost:8888' and enter password 'jupyter'. 

* upload the notebooks from `/notebooks`