package org.infinispan.client.openapi;

import java.net.http.HttpClient;
import java.util.concurrent.ExecutorService;

import org.infinispan.client.openapi.configuration.OpenAPIClientConfiguration;

public class OpenAPIClient implements AutoCloseable {

   private boolean managedExecutorService;
   private ExecutorService executorService;
   private HttpClient httpClient;

   public static OpenAPIClient forConfiguration(OpenAPIClientConfiguration configuration) {
      return null;
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

}
