package org.infinispan.graalvm.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import com.sun.net.httpserver.HttpServer;

/**
 * A stub of the Kubernetes API server which serves canned JSON responses for the handful of endpoints used by the
 * <code>kubectl-infinispan</code> commands. It exists so that those commands can be exercised without a real cluster.
 *
 * @since 16.3
 */
final class FakeKubeApiServer implements AutoCloseable {

   private final String namespace;
   private final HttpServer server;

   private FakeKubeApiServer(String namespace, HttpServer server) {
      this.namespace = namespace;
      this.server = server;
   }

   /**
    * Creates and starts a stub API server, bound to an ephemeral port on the loopback interface.
    *
    * @param namespace the namespace in which the stub resources live.
    */
   static FakeKubeApiServer start(String namespace) throws IOException {
      HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
      routes(namespace).forEach((path, body) -> server.createContext(path, exchange -> {
         byte[] payload = body.getBytes(StandardCharsets.UTF_8);
         exchange.getResponseHeaders().add("Content-Type", "application/json");
         exchange.sendResponseHeaders(200, payload.length);
         try (var out = exchange.getResponseBody()) {
            out.write(payload);
         }
      }));
      server.start();
      return new FakeKubeApiServer(namespace, server);
   }

   /**
    * @return the contents of a <code>kubeconfig</code> which points at this server.
    */
   String kubeConfig() {
      return """
            apiVersion: v1
            kind: Config
            current-context: stub
            clusters:
            - name: stub
              cluster:
                server: http://127.0.0.1:%d
            contexts:
            - name: stub
              context:
                cluster: stub
                namespace: %s
                user: stub
            users:
            - name: stub
              user: {}
            """.formatted(server.getAddress().getPort(), namespace);
   }

   @Override
   public void close() {
      server.stop(0);
   }

   private static Map<String, String> routes(String namespace) {
      Map<String, String> routes = new LinkedHashMap<>();
      routes.put("/version", """
            {"major":"1","minor":"33","gitVersion":"v1.33.0","platform":"linux/amd64"}
            """);
      routes.put("/apis/infinispan.org/v1/namespaces/" + namespace + "/infinispans", """
            {"apiVersion":"infinispan.org/v1","kind":"InfinispanList","items":[
              {"apiVersion":"infinispan.org/v1","kind":"Infinispan",
               "metadata":{"name":"infinispan","namespace":"%s"},
               "spec":{"replicas":1,"security":{"endpointSecretName":"infinispan-generated-secret"}}}
            ]}
            """.formatted(namespace));
      routes.put("/api/v1/namespaces/" + namespace + "/pods", """
            {"apiVersion":"v1","kind":"PodList","items":[
              {"apiVersion":"v1","kind":"Pod",
               "metadata":{"name":"infinispan-0","namespace":"%s","labels":{"infinispan_cr":"infinispan"}},
               "spec":{"containers":[{"name":"infinispan","ports":[{"name":"infinispan","containerPort":11222}]}]},
               "status":{"phase":"Running"}}
            ]}
            """.formatted(namespace));
      routes.put("/api/v1/namespaces/" + namespace + "/secrets/infinispan-generated-secret", """
            {"apiVersion":"v1","kind":"Secret","type":"Opaque",
             "metadata":{"name":"infinispan-generated-secret","namespace":"%s"},
             "data":{"identities.yaml":"Y3JlZGVudGlhbHM6Ci0gdXNlcm5hbWU6IGFkbWluCiAgcGFzc3dvcmQ6IHBhc3N3b3JkCgo="}}
            """.formatted(namespace));
      return routes;
   }
}
