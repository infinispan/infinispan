Infinispan is a distributed in-memory key/value data store that provides remote caches through Hot Rod, REST, Memcached, and RESP endpoints, with built-in monitoring and administration.
This image runs the Infinispan Server in a dedicated JVM and exposes all endpoints on the single port `11222`.

Start a single node:

```bash
docker run -p 11222:11222 --name infinispan infinispan/server
```

The image has authentication enabled on all endpoints and prints an auto-generated username/password pair with the `admin` role to stdout on first start.
Alternatively, provide credentials with the `USER` and `PASS` environment variables:

```bash
docker run -p 11222:11222 -e USER="admin" -e PASS="changeme" --name infinispan infinispan/server
```

The web console is available at http://localhost:11222/console.
Hot Rod clients connect to `localhost:11222` using the SASL `DIGEST-SHA-256` mechanism.

You can customize the JVM with `JAVA_OPTIONS`, deploy artifacts to the server lib directory with `SERVER_LIBS`, and define users and roles with `IDENTITIES_BATCH`.
Clustering in Kubernetes requires the JGroups DNS_PING discovery protocol.

Refer to the container image guide for the complete documentation: https://infinispan.org/docs/stable/titles/container_image/container_image.html
