```

NS=default

# KRaft cluster id (pick one, keep forever for this cluster)
CLUSTER_ID=$(kubectl -n $NS run gen-kid --rm -i --restart=Never \
  --image=docker.io/bitnamilegacy/kafka:4.0.0-debian-12-r10 --quiet -- \
  /opt/bitnami/kafka/bin/kafka-storage.sh random-uuid)
kubectl -n $NS create secret generic kafkab-kraft \
  --from-literal=cluster-id="$CLUSTER_ID" \
  --dry-run=client -o yaml | kubectl apply -f -


# Your PEMs
kubectl -n $NS create secret generic kafka-pem \
  --from-file=tls.crt=certificate.pem \
  --from-file=tls.key=key.pem \
  --from-file=ca.crt=caroot.pem

PW='xxx'
kubectl -n $NS create secret generic kafkab-tls-passwords \
  --from-literal=keystore-password="$PW" \
  --from-literal=truststore-password="$PW" \
  --dry-run=client -o yaml | kubectl apply -f -


```


```commandline
kubectl -n $NS apply -f kafkab.yaml
```



