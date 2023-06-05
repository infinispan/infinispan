package org.infinispan.server.test.api;

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfiguration;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.test.core.TestClient;
import org.infinispan.server.test.core.TestServer;

import okhttp3.ConnectionPool;
import okhttp3.OkHttpClient;
import okhttp3.internal.connection.RealConnectionPool;

/**
 * REST operations for the testing framework
 *
 * @author Tristan Tarrant
 * @since 10
 */
public class RestTestClientDriver extends BaseTestClientDriver<RestTestClientDriver> {
   public static final int TIMEOUT = Integer.getInteger("org.infinispan.test.server.http.timeout", 10);

   private RestClientConfigurationBuilder clientConfiguration = new RestClientConfigurationBuilder();
   private final TestServer testServer;
   private final TestClient testClient;
   private int port = 11222;

   public RestTestClientDriver(TestServer testServer, TestClient testClient) {
      this.testServer = testServer;
      this.testClient = testClient;
   }

   /**
    * Provide a custom client configuration to connect to the server via REST
    *
    * @param clientConfiguration
    * @return the current {@link RestTestClientDriver} instance with the rest client configuration override
    */
   public RestTestClientDriver withClientConfiguration(RestClientConfigurationBuilder clientConfiguration) {
      this.clientConfiguration = clientConfiguration;
      return this;
   }

   public RestTestClientDriver withPort(int port) {
      this.port = port;
      return this;
   }

   /**
    * Create and get a REST client.
    *
    * @return a new instance of the {@link RestClient}
    */
   public RestClient get() {
      return testClient.registerResource(new OkHttpCloseable(testServer.newRestClient(clientConfiguration, port))).client;
   }

   /**
    * Create and get a REST client that is connected to the Nth server of the cluster.
    *
    * @return a new instance of the {@link RestClient}
    */
   public RestClient get(int n) {
      return testClient.registerResource(new OkHttpCloseable(testServer.newRestClientForServer(clientConfiguration, port, n))).client;
   }

   /**
    * Create a new REST client and create a cache whose name will be the test name where this method
    * is called from.
    *
    * @return new {@link RestClient} instance
    */
   public RestClient create() {
      RestClient restClient = get();
      String name = testClient.getMethodName(qualifier);
      CompletionStage<RestResponse> future;
      if (serverConfiguration != null) {
         RestEntity configEntity = RestEntity.create(MediaType.APPLICATION_XML, serverConfiguration.toStringConfiguration(name));
         future = restClient.cache(name).createWithConfiguration(configEntity, flags.toArray(new CacheContainerAdmin.AdminFlag[0]));
      } else if (mode != null) {
         future = restClient.cache(name).createWithTemplate("org.infinispan." + mode.name(), flags.toArray(new CacheContainerAdmin.AdminFlag[0]));
      } else {
         future = restClient.cache(name).createWithTemplate("org.infinispan." + CacheMode.DIST_SYNC.name(), flags.toArray(new CacheContainerAdmin.AdminFlag[0]));
      }
      try (RestResponse response = Exceptions.unchecked(() -> future.toCompletableFuture().get(TIMEOUT, TimeUnit.SECONDS))) {
         if (response.getStatus() != 200) {
            switch (response.getStatus()) {
               case 400:
                  throw new IllegalArgumentException("Bad request while attempting to obtain rest client: " + response.getStatus());
               case 401:
               case 403:
                  throw new SecurityException("Authentication error while attempting to obtain rest client = " + response.getStatus());
               default:
                  throw new RuntimeException("Could not obtain rest client = " + response.getStatus());
            }
         } else {
            // If the request succeeded without authn but we were expecting to authenticate, it's an error
            if (restClient.getConfiguration().security().authentication().enabled() && !response.usedAuthentication()) {
               throw new SecurityException("Authentication expected but anonymous access succeeded");
            }
            return restClient;
         }
      }
   }

   public static RestClient forConfiguration(RestClientConfiguration cfg) {
      return new OkHttpCloseable(RestClient.forConfiguration(cfg)).client;
   }

   @Override
   public RestTestClientDriver self() {
      return this;
   }

   private static class OkHttpCloseable implements Closeable {
      private final RestClient client;
      private final ExecutorService executor;

      private OkHttpCloseable(RestClient client) {
         this.client = client;
         this.executor = okHttpRealConnectionOperate();
      }

      private ExecutorService okHttpRealConnectionOperate() {
         OkHttpClient httpClient = extractField(client, "httpClient");
         ConnectionPool cp = httpClient.connectionPool();
         RealConnectionPool rcp = extractField(cp, "delegate");

         // The original runnable would block with `wait`.
         replaceField(RealConnectionPool.class, rcp,"cleanupRunnable", prev -> (Runnable) () -> { });
         Executor og = replaceField(RealConnectionPool.class, rcp, "executor", ignore -> r -> { });
         if (og instanceof ExecutorService) {
            return (ExecutorService) og;
         }
         return null;
      }

      @Override
      public void close() throws IOException {
         client.close();
         if (executor != null) {
            executor.shutdown();
         }
      }

      private static <T> T replaceField(Class<?> baseType, Object owner, String fieldName, Function<T, T> func) {
         Field field;
         try {
            field = baseType.getDeclaredField(fieldName);
            field.setAccessible(true);

            MethodHandles.Lookup lookup = MethodHandles.privateLookupIn(Field.class, MethodHandles.lookup());
            VarHandle modifiersField = lookup.findVarHandle(Field.class, "modifiers", int.class);
            modifiersField.set(field, field.getModifiers() & ~Modifier.FINAL);

            T prevValue = (T) field.get(owner);
            Object newValue = func.apply(prevValue);
            field.set(owner, newValue);
            return prevValue;
         } catch (Exception e) {
            throw new RuntimeException(e);
         }
      }

      private static <T> T extractField(Object target, String fieldName) {
         return extractField(target.getClass(), target, fieldName);
      }

      private static <T> T extractField(Class<?> type, Object target, String fieldName) {
         while (true) {
            Field field;
            try {
               field = type.getDeclaredField(fieldName);
               field.setAccessible(true);
               return (T) field.get(target);
            } catch (Exception e) {
               if (type.equals(Object.class)) {
                  throw new RuntimeException(e);
               } else {
                  // try with superclass!!
                  type = type.getSuperclass();
               }
            }
         }
      }
   }
}
