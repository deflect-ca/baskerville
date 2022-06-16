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
* Create `admin` and `client` TSL keys 
```commandline
mkdir kafka
cd kafka
mkdir keys
git clone https://github.com/confluentinc/confluent-platform-security-tools
cd /confluent-platform-security-tools
```
- enter path of trust store file: ./kafka.truststore.jks
- enter path of the trust store's private key: ./truststore/ca-key
- enter new user password
- enter `admin` for admin key, `user` for user key when asked for `CN=`
- enter key password for <localhost> : just Enter
- enter root truststore password
- enter new user password
- enter new user password again
- answer yes
- enter new user password again
- answer yes to delete
```commandline
mv ./keystore/kafka.keystore.jks ./admin_or_user.jks
rm -r keystore/
```
* Create a pod for ACL commands
```commandline
kubectl run kafka-client --restart='Never' --image docker.io/bitnami/kafka:2.8.0-debian-10-r43 --env="ALLOW_PLAINTEXT_LISTENER=yes" --namespace default --command -- sleep infinity
```
* login to the `kafka-client` pod
```commandline
kubectl exec --tty -i kafka-client --namespace default -- bash
```
* set the client access rights(only `write` permissions to `features` topic)
```commandline
kafka-acls.sh \
  --bootstrap-server kafka-0.kafka-headless.default.svc.cluster.local:9093 \
  --add \
  --allow-principal User:ANONYMOUS \
  --operation Write \
  --topic 'features' \
  --topic 'deflect.logs' \
  --topic 'predictions' \
  --topic 'banjax_command_topic' \
  --topic 'banjax_report_topic'

kafka-acls.sh \
  --bootstrap-server kafka-0.kafka-headless.default.svc.cluster.local:9093 \
  --add \
  --allow-principal User:ANONYMOUS \
  --operation Read \
  --topic 'features' \
  --topic 'deflect.logs' \
  --topic 'predictions'\
  --topic 'banjax_command_topic' \
  --topic 'banjax_report_topic'

kafka-acls.sh \
  --bootstrap-server kafka-0.kafka-headless.default.svc.cluster.local:9093 \
  --add \
  --allow-principal User:ANONYMOUS \
  --operation Describe \
  --topic 'features' \
  --topic 'deflect.logs' \
  --topic 'predictions'\
  --topic 'banjax_command_topic' \
  --topic 'banjax_report_topic'

kafka-acls.sh \
  --bootstrap-server kafka-0.kafka-headless.default.svc.cluster.local:9093 \
  --add \
  --allow-principal User:CN=client,OU=Unknown,O=Unknown,L=Unknown,ST=Unknown,C=Unknown \
  --operation Write \
  --topic 'features' 

kafka-acls.sh \
  --bootstrap-server kafka-0.kafka-headless.default.svc.cluster.local:9093 \
  --add \
  --allow-principal User:CN=client,OU=Unknown,O=Unknown,L=Unknown,ST=Unknown,C=Unknown \
  --operation Describe \
  --topic 'deflect.logs' \
  --topic 'banjax_command_topic' 
```
* verify ACL permissions:
```commandline
kafka-acls.sh --bootstrap-server kafka-0.kafka-headless.default.svc.cluster.local:9093 --list
```
## JKS to PEM conversion
Baskerville uses PEM files in order to write to Kafka topics from Python.
Every new client should be provided with PEM files rather than with JKS files 
generated in the previous step. 
Below is the instruction how to convert JKS to PEM:

* `caroot.pem`:
keytool -importkeystore -srckeystore kafka.truststore.jks -destkeystore kafka.truststore.p12 -deststoretype PKCS12
openssl pkcs12 -in kafka.truststore.p12 -nokeys -out caroot.pem
* `admin.key.pem` and `admin.certificate.pem`:
keytool -importkeystore -srckeystore admin.jks -destkeystore admin.p12 -deststoretype PKCS12
openssl pkcs12 -in admin.p12 -nokeys -out admin.certificate.pem
openssl pkcs12 -in admin.p12 -nodes -nocerts -out admin.key.pem
* `client.key.pem` and `client.certificate.pem`:
keytool -importkeystore -srckeystore client.jks -destkeystore client.p12 -deststoretype PKCS12
openssl pkcs12 -in client.p12 -nokeys -out client.certificate.pem
openssl pkcs12 -in client.p12 -nodes -nocerts -out client.key.pem

* delete everything from all *.certificate.pem except 'localhost' friendly name certificate

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

* port forwarding
```
kubectl port-forward service/postgres-db-lb 5432:5432
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
docker build . -t equalitie/baskerville:notebook_base -f ./deployment/notebook/Dockerfile
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

* if you made apply your local changes to an image, you can build it with
```commandline
docker build . -f ./deployment/notebook/local/Dockerfile -t equalitie/baskerville:notebook 
docker push equalitie/baskerville:notebook
```

* create notebook port forwarding
```commandline
kubectl port-forward -n spark deployment.apps/notebook-deployment 8888:8888
````

* open `localhost:8888' and enter password 'jupyter'. 

* upload the notebooks from `/notebooks`

## Onboarding new Baskerville client
* provide clearinghouse Kafka connection string
* provide client TLS keys for writing to `features` topic in clearinghouse Kafka:
`client.key.pem`, `client.certificate.pem`, `caroot.pem`
* provide kafka truststore `kafka.truststore.jks`
* create a new client set of Kafka jks and provide `kafka.keystore.jks`
* request the client IP
* create a new client name and register the new client in `predicting.yaml`
```yaml
kafka:
  client_connections:
    new_client_name:
      bootstrap_servers: 'client_ip:29092'
      security_protocol: 'SSL'
      ssl_check_hostname: False
      ssl_cafile: '/usr/local/baskerville/kafka/caroot.pem'
      ssl_certfile: '/usr/local/baskerville/kafka/admin.certificate.pem'
      ssl_keyfile: '/usr/local/baskerville/kafka/admin.key.pem'
      api_version: '0.11.5'
```

## Install KSQL
* clone the repo `git@github.com:confluentinc/cp-helm-charts.git`
```
cd baskerville
cd ..
git clone git@github.com:confluentinc/cp-helm-charts.git
```
* install schema registry
```commandline
helm install ksql-schema-registry -f deployment/ksql/values-ksql-registry.yaml ../cp-helm-charts/charts/cp-schema-registry
```

* install `ksql`
```commandline
helm install ksql -f deployment/ksql/values-ksql.yaml ../cp-helm-charts/charts/cp-ksql-server
```

* connect to ksql cli to confirm the deployment
```commandline
kubectl run ksql-cli --rm -i --tty --image confluentinc/cp-ksql-cli:5.2.1 http://ksql-cp-ksql-server:8088
```
or 
```commandline
kubectl attach ksql-cli -c ksql-cli -i -t
```

To make sure KSQL is up and running you can list kafka topics inside KSQL:
```commandline
shot topics;
```

* create the cstat KSQL queries. Copy the content of `./deployment/ksql/create_queries.sql`
and paste it inside KSQL cli pod.
Make sure you don't have any errors.

* to check KSQL logs:
List the pods:
```commandline
kubectl get pods
```
Locate one of the ksql pods, for example, ksql-cp-ksql-server-5b7466c57f-89vx5
Get the logs:
```commandline
kubectl logs ksql-cp-ksql-server-5b7466c57f-89vx5 cp-ksql-server --since=5m
```

* To change retention policy of cstats topics to 24 hours:
login go kafka-client pod:
```commandline
kubectl run kafka-client --restart='Never' --image docker.io/bitnami/kafka:2.8.0-debian-10-r43 --namespace default --command -- 
or
kubectl exec --tty -i kafka-client --namespace default -- bash
```
change the retention policy:
```commandline
kafka-topics.sh --zookeeper kafka-zookeeper-headless:2181 --alter --topic STATS_WEBLOGS_5M --config retention.ms=86400000
kafka-topics.sh --zookeeper kafka-zookeeper-headless:2181 --alter --topic STATS_BANJAX_5M --config retention.ms=86400000
```

## Uninstalling KSQL

* stop the helm charts:
```commandline
helm delete ksql
helm delete ksql-schema-registry
```

* delete kafka topics:
```commandline
kubectl run kafka-client --restart='Never' --image docker.io/bitnami/kafka:2.8.0-debian-10-r43 --namespace default --command -- 
```
or
```
kubectl exec --tty -i kafka-client --namespace default -- bash
```
then inside kafka-client pod:
```
kafka-topics.sh --zookeeper kafka-zookeeper-headless:2181 --delete --topic '_confluent-ksql-.*'
kafka-topics.sh --zookeeper kafka-zookeeper-headless:2181 --delete --topic 'STATS_.*'
kafka-topics.sh --zookeeper kafka-zookeeper-headless:2181 --delete --topic _schemas
```

It might be also necessary to re-partition the topics which were auto-created after the previous step.
```commandline
kafka-topics.sh --zookeeper kafka-zookeeper-headless:2181 --alter --topic STATS_WEBLOGS_5M --partitions 3 
kafka-topics.sh --zookeeper kafka-zookeeper-headless:2181 --alter --topic STATS_BANJAX_5M --partitions 3 
kafka-topics.sh --zookeeper kafka-zookeeper-headless:2181 --alter --topic STATS_BANJAX --partitions 3 
kafka-topics.sh --zookeeper kafka-zookeeper-headless:2181 --alter --topic STATS_BANJAX_WWW --partitions 3 
```

## KStream


mvn compile jib:build

cd deployment/kafka_stream
kubectl create -f ./deployment/kafka-stream/baskerville-streams-deployment.yaml
kubectl delete -f ./deployment/kafka-stream/baskerville-streams-deployment.yaml

