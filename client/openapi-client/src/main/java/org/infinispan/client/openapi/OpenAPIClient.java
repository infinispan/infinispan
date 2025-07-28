package org.infinispan.client.openapi;

import java.net.http.HttpClient;
import java.util.concurrent.ExecutorService;

import org.infinispan.client.openapi.api.CacheApi;
import org.infinispan.client.openapi.configuration.OpenAPIClientConfiguration;

public class OpenAPIClient implements AutoCloseable {
   private final OpenAPIClientConfiguration configuration;
   private final ApiClient apiClient;
   private final CacheApi cacheApi;
   private boolean managedExecutorService;
   private ExecutorService executorService;
   private HttpClient httpClient;

   public OpenAPIClient(OpenAPIClientConfiguration configuration) {
      this.configuration = configuration;
      apiClient = new ApiClient();
      cacheApi = new CacheApi(apiClient);
   }

   public CacheApi cache() {
      return cacheApi;
   }

   public static OpenAPIClient forConfiguration(OpenAPIClientConfiguration configuration) {
      return new OpenAPIClient(configuration);
   }

   @Override
   public void close() throws Exception {
      if (Runtime.version().compareTo(Runtime.Version.parse("21")) >= 0) {
         ((AutoCloseable) httpClient).close(); // close() was only introduced in JDK 21
      }
      if (managedExecutorService) {
         executorService.shutdownNow();
      }
   }

   public OpenAPIClientConfiguration getConfiguration() {
      return configuration;
   }
}
