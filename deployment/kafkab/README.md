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




## ACL (Nov 2026)

The edge certificate leaked, and every edge uses the same one, so ACLs cannot tell edges apart. They
limit what that certificate can do instead: edges (banjax) may read commands and write reports, and
cannot send commands. Everything inside the cluster uses PLAINTEXT listeners as `User:ANONYMOUS`,
which is a super user.

| Principal | Listener | Rights |
|---|---|---|
| `User:ANONYMOUS` | CONTROLLER 9091, INTERNAL 9092, CLIENT 9093 (PLAINTEXT, in-cluster) | super user |
| `User:CN=Unknown,OU=Unknown,O=Unknown,L=Unknown,ST=Unknown,C=Unknown` | EXTERNAL 9094 (mTLS, edges) | Read+Describe `banjax_command_topic`, Write+Describe `banjax_report_topic` |

banjax reads its dnet's partition without a consumer group, so it needs no group ACL.

### Phase 1: authorizer on, nothing denied yet

`kafkab.yaml` sets `authorizer.class.name=StandardAuthorizer`, `super.users=User:ANONYMOUS`,
`allow.everyone.if.no.acl.found=true`. Without ACLs this changes nothing.

    kubectl apply -f kafkab.yaml && kubectl rollout status statefulset/kafkab

Then add the edge ACLs (as ANONYMOUS on the internal listener). From this moment only the cluster
can write to `banjax_command_topic`; other topics stay open until phase 2.

    P='User:CN=Unknown,OU=Unknown,O=Unknown,L=Unknown,ST=Unknown,C=Unknown'
    A="/opt/bitnami/kafka/bin/kafka-acls.sh --bootstrap-server localhost:9092"
    kubectl exec kafkab-0 -c kafka -- $A --add --allow-principal "$P" --operation Read  --operation Describe --topic banjax_command_topic
    kubectl exec kafkab-0 -c kafka -- $A --add --allow-principal "$P" --operation Write --operation Describe --topic banjax_report_topic
    kubectl exec kafkab-0 -c kafka -- $A --list

Check: banjax on the edges still receives commands (Kibana `deflect.log-*`, `banjax_decision` reacts
to a manual `challenge_ip`), and the brokers log no unexpected denials:

    kubectl logs kafkab-0 -c kafka | grep -i "denied"

A denial for `User:CN=Unknown…` on Write `banjax_command_topic` means some external client was
sending commands: find out who before going on.

### Phase 2: deny everything else to the edge certificate

Set `allow.everyone.if.no.acl.found=false` in `kafkab.yaml`, apply, wait for the rollout, check again.

### Rollback

Remove the three authorizer lines from `kafkab.yaml` and apply. ACLs stay stored in the metadata but
are ignored without an authorizer. To remove them: `$A --remove --allow-principal "$P" --topic <topic>`.
