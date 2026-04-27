package test.org.infinispan.spring.starter.remote.schema;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.commons.configuration.StringConfiguration;
import org.infinispan.server.test.api.TestUser;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.infinispan.spring.starter.remote.InfinispanRemoteAutoConfiguration;
import org.infinispan.spring.starter.remote.InfinispanRemoteCacheManagerAutoConfiguration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfigurationPackage;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Verifies that Protobuf schemas are auto-discovered and registered with the server.
 *
 * @since 16.2
 */
@SpringBootTest(
      classes = {
            InfinispanRemoteAutoConfiguration.class,
            InfinispanRemoteCacheManagerAutoConfiguration.class,
            GreetingController.class
      },
      webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
      properties = {
            "spring.main.banner-mode=off",
            "infinispan.remote.cache.greetings.marshaller=org.infinispan.commons.marshall.ProtoStreamMarshaller"
      }
)
@EnableAutoConfiguration
@AutoConfigurationPackage
public class SchemaAutoRegistrationIT {

   @RegisterExtension
   static InfinispanServerExtension SERVER = InfinispanServerExtensionBuilder.server();

   @DynamicPropertySource
   static void infinispanProperties(DynamicPropertyRegistry registry) {
      InetSocketAddress serverAddress = SERVER.getTestServer().getDriver().getServerSocket(0, 11222);
      registry.add("infinispan.remote.server-list",
            () -> serverAddress.getHostString() + ":" + serverAddress.getPort());
      registry.add("infinispan.remote.auth-username", () -> TestUser.ADMIN.getUser());
      registry.add("infinispan.remote.auth-password", () -> TestUser.ADMIN.getPassword());
   }

   @Autowired
   private RemoteCacheManager remoteCacheManager;

   @LocalServerPort
   private int port;

   @BeforeAll
   static void createCache() {
      InetSocketAddress serverAddress = SERVER.getTestServer().getDriver().getServerSocket(0, 11222);
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder builder =
            new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
      builder.addServer().host(serverAddress.getHostString()).port(serverAddress.getPort());
      builder.security().authentication()
            .username(TestUser.ADMIN.getUser())
            .password(TestUser.ADMIN.getPassword());

      try (RemoteCacheManager rcm = new RemoteCacheManager(builder.build())) {
         rcm.administration().getOrCreateCache(GreetingController.CACHE_NAME,
               new StringConfiguration(
                     "<distributed-cache>" +
                           "<encoding media-type=\"application/x-protostream\"/>" +
                           "</distributed-cache>"));
      }
   }

   @Test
   void schemaIsAutoRegisteredOnServer() {
      assertThat(remoteCacheManager.administration().schemas()
            .exists("GreetingSchema.proto")).isTrue();
   }

   @Test
   void putAndGetViaRestApi() throws Exception {
      HttpClient client = HttpClient.newHttpClient();
      String baseUrl = "http://localhost:" + port + "/api/greetings";

      // PUT a greeting
      HttpRequest putRequest = HttpRequest.newBuilder()
            .uri(URI.create(baseUrl + "/1"))
            .header("Content-Type", "application/json")
            .PUT(HttpRequest.BodyPublishers.ofString("{\"id\":\"1\",\"message\":\"Kaixo Mundua!\"}"))
            .build();
      HttpResponse<String> putResponse = client.send(putRequest, HttpResponse.BodyHandlers.ofString());
      assertThat(putResponse.statusCode()).isEqualTo(200);

      // GET the greeting
      HttpRequest getRequest = HttpRequest.newBuilder()
            .uri(URI.create(baseUrl + "/1"))
            .GET()
            .build();
      HttpResponse<String> getResponse = client.send(getRequest, HttpResponse.BodyHandlers.ofString());
      assertThat(getResponse.statusCode()).isEqualTo(200);
      assertThat(getResponse.body()).contains("Kaixo Mundua!");
   }

   @Test
   void putAndGetViaRemoteCache() {
      RemoteCache<String, Greeting> cache = remoteCacheManager.getCache(GreetingController.CACHE_NAME);
      assertThat(cache).isNotNull();

      Greeting greeting = new Greeting("2", "Agur!");
      cache.put("2", greeting);

      Greeting retrieved = cache.get("2");
      assertThat(retrieved).isNotNull();
      assertThat(retrieved.id()).isEqualTo("2");
      assertThat(retrieved.message()).isEqualTo("Agur!");
   }

   @Test
   void getNonExistentReturns404() throws Exception {
      HttpClient client = HttpClient.newHttpClient();
      HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:" + port + "/api/greetings/non-existent"))
            .GET()
            .build();
      HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
      assertThat(response.statusCode()).isEqualTo(404);
   }
}
