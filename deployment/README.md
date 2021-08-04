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
* create `baskerville_secrets.yaml
*
```
apiVersion: v1
kind: Secret
metadata:
  name: baskerville-secrets
type: Opaque
stringData:
  s3_access: "your_s3_access"
  s3_secret: "your_s3_secret"
  redis_password: "your_redis_password"
  postgres_host: ""
  postgres_user: "postgres"
  kafka_host: "kafka-0.kafka-headless.default.svc.cluster.local:9093,kafka-1.kafka-headless.default.svc.cluster.local:9093,kafka-2.kafka-headless.default.svc.cluster.local:9093"
  postgres_password: "your_postgres_password"
```
* create secret
```
k apply -f baskerville_secrets.yaml
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
```commandline
helm install postgres -f deployment/postgres/values-postgres.yaml bitnami/postgresql

```



* deploy timescale db:
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

## Jupyter Hub

* edit your password and node selector in `deployment/jupyter/values-jupyter.yaml`

* deploy Jupyter Hub
```commandline
helm install jupyter -f deployment/jupyterhub/values-jupyterhub.yaml bitnami/jupyterhub
```

  
kubectl port-forward service/jupyter-jupyterhub-proxy-public 8080:80

## Run Baskerville
* Add a Kafka output to your logstash instance. You should use the external kafka connection url you created. 
See an example in [Baskerville Client repository](https://github.com/equalitie/baskerville_client)
* Launch `Preprocessing`, `Predicting`, `Postprocessing` workflows.
* Launch `Training' workflow to retrain the model.

