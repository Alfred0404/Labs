# Lab 6.1 - Kubernetes basics with Minikube

This lab use Minikube with Docker driver. The Kubernetes version used for the test is 1.37.0.

## Cluster and metrics

The Minikube node was in `Ready` status. The command below show CPU and memory of all pods, sort them by CPU and add the total at the end.

```bash
kubectl top pods -A --sort-by cpu --sum=true
```

The pods in `kube-system` have these roles:

- `kube-apiserver` receive the requests for the cluster.
- `etcd` save the cluster data.
- `kube-scheduler` choose a node for a new pod.
- `kube-controller-manager` verify the real state is like the wanted state.
- `kube-proxy` manage the network for services.
- `coredns` give DNS names inside the cluster.
- `kindnet` manage the network between pods.
- `metrics-server` collect CPU and memory values.
- `storage-provisioner` can create local volumes.

## First deployment

I created `kubernetes-bootcamp` with one pod. The application file is `/server.js` and it listen on port 8080.

The request from inside the pod worked:

```text
Hello Kubernetes bootcamp! | Running on: kubernetes-bootcamp-... | v=1
```

The pod IP was not accessible directly from Windows because the pod is inside the Minikube network.

## Service

I exposed the deployment with a `NodePort` service on port 8080. With Docker driver on Windows, I used `minikube service kubernetes-bootcamp --url` for create a local tunnel. The application was accessible with this URL.

## Scaling

I used this command for verify the five pods:

```bash
kubectl get pods -l app=kubernetes-bootcamp
```

The ten requests returned different pod names. It happen because the service distribute the connections between available pods. After, I scaled down to two replicas and the three other pods were terminated.

## Rolling update

During update from v1 to v2, the old pods stayed available until new pods were ready. The responses changed progressively from `v=1` to `v=2`. Kubernetes service stayed available. One request on the local Minikube tunnel failed, but the pods were still ready.

The v3 image does not exist. The new pod stayed in `ImagePullBackOff`, but two v2 pods continued to answer. I cancelled this update with `kubectl rollout undo`, then I used revision 1 for return to the first v1 image.

## YAML manifests

The deployment manifest create three replicas with the v1 image. The service select pods with the label `app: kubernetes-bootcamp` and expose container port 8080.

```bash
kubectl apply -f deployment.yaml
kubectl apply -f service.yaml
```

The service answered correctly and the requests used different replicas.
