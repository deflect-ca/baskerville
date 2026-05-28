```

NS=default

# 0a) KRaft cluster id (pick one, keep forever for this cluster)
CLUSTER_ID=$(kubectl -n $NS run gen-kid --rm -i --restart=Never \
  --image=docker.io/bitnamilegacy/kafka:4.0.0-debian-12-r10 --quiet -- \
  /opt/bitnami/kafka/bin/kafka-storage.sh random-uuid)
kubectl -n $NS create secret generic kafka9-kraft \
  --from-literal=cluster-id="$CLUSTER_ID" \
  --dry-run=client -o yaml | kubectl apply -f -


# 0b) Your PEMs
kubectl -n $NS create secret generic kafka-pem2 \
  --from-file=tls.crt=certificate.pem \
  --from-file=tls.key=key.pem \
  --from-file=ca.crt=caroot.pem

PW='B1^ZRUUVoIuKND7t2HiJ8fwRg0kdMo4zdh8m8eRzgXw!'
kubectl -n $NS create secret generic kafka9-tls-passwords \
  --from-literal=keystore-password="$PW" \
  --from-literal=truststore-password="$PW" \
  --dry-run=client -o yaml | kubectl apply -f -


```


```commandline
kubectl -n $NS apply -f kafka9.yaml
```



