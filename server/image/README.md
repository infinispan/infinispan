# Infinispan Images

This repository contains various artifacts to create Infinispan Server images.

The container descriptor uses a multi-stage approach to create a minimal, hardened, and secure container images, with a
significantly reduced attack surface.

## Building the images

### With Maven

```
mvn install -P image -pl server/image/
```

### With Docker

```
docker buildx build \
    --build-arg BRAND_VERSION=16.0.0.Dev06 \
    --build-arg JDK_DIST=src/main/docker/empty \
    --build-arg SERVER_DIST=https://github.com/infinispan/infinispan/releases/download/16.0.0.Dev06/infinispan-server-16.0.0.Dev06.zip \
    -f server/image/src/main/docker/Dockerfile \
    --tag infinispan/server:16.0.0.Dev06 \
    .
```

### Build arguments

The following build arguments are supported:

| Docker build arg  | Maven system property      | Description                                                                                                                                                                           |
|-------------------|----------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `BASE_IMAGE`      | N/A                        | the base image used by the container image (defaults to `scratch`)                                                                                                                    |
| `BRAND_NAME`      | `infinispan.brand.name`    | the brand name used by the container image labels (defaults to `Infinispan`)                                                                                                          |
| `BRAND_VERSION`   | `infinispan.brand.version` | the brand version used by the container image labels (required)                                                                                                                       |
| `JDK_DIST`        | `jdk.dist`                 | a local or remote `zip`, `tar.gz`, or `rpm` distribution of OpenJDK. If not specified, the Temuring OpenJDK will be donwloaded from Adoptium. Ignored if `JAVA_PACKAGE` is specified. |
| `OPENJDK_PACKAGE` | N/A                        | the name of an OpenJDK package provided by the builder repository (currently RHEL 10.x) (optional)                                                                                    |
| `SERVER_DIST`     | `server.dist`              | a local or remote `zip` distribution of Infinispan Server (required)                                                                                                                  |

