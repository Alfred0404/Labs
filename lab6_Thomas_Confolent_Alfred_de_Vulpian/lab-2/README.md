# Lab 6.2 - ConfigMap and Secret

This lab use the namespace `lab-configmap-secret` for separate the tests from other resources.

## ConfigMaps

I created ConfigMaps with three methods: from a properties file, from literal values and from a YAML manifest. A ConfigMap is for configuration who is not sensitive. The values can be read directly with `kubectl`.

The `data-platform-config` contains simple values and also two complete files. The `spark-app` pod receive values as environment variables. The `spark-config-reader` pod receive `application.yaml` and `tables.csv` in `/etc/config`.

## Secrets

I created Secrets from literal values, from a file and from a temporary YAML manifest. Kubernetes show the values encoded in base64, but it is not encryption. For this reason, the real credentials files are in `.gitignore` and are deleted after the test. The `.example` files have only placeholders and can stay in Git.

The `ceph-connector` pod receive Secret values as environment variables. The `ceph-connector-secure` pod receive the credentials as a read-only volume with permission `0600`.

In a real project, we must not print credentials in logs and we must not commit a Secret with clear values. We can use a vault, Sealed Secrets or an external secret manager.

## Commands

```bash
kubectl create namespace lab-configmap-secret
kubectl -n lab-configmap-secret create configmap pipeline-config --from-file=pipeline-config.properties
kubectl apply -f configmap-data-platform.yaml
cp s3-credentials.example.txt s3-credentials.txt
cp secret-ceph-credentials.example.yaml secret-ceph-credentials.yaml
kubectl -n lab-configmap-secret create secret generic s3-credentials --from-file=s3-credentials.txt
kubectl apply -f secret-ceph-credentials.yaml
kubectl apply -f pod-with-configmap.yaml
kubectl apply -f pod-with-configmap-volume.yaml
kubectl apply -f pod-with-secret.yaml
kubectl apply -f pod-with-secret-volume.yaml
```

All environment variables and mounted files were verified inside the pods. At the end, I deleted the namespace for remove all resources of the lab.
