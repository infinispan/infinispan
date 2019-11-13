package org.infinispan;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Base64;

/**
 * RestExample class shows you how to access your cache via HttpClient API with Java 11 or later.
 *
 * @author Gustavo Lira (glira@redhat.com)
 */
public class RestExample {
   private static final String SERVER_ADDRESS = "http://localhost:11222";
   private static final String CACHE_URI = "/rest/v2/caches/default";

   /**
    * postMethod create a named cache.
    * @param httpClient HTTP client that sends requests and receives responses
    * @param builder    Encapsulates HTTP requests
    * @throws IOException
    * @throws InterruptedException
    */
   public void postMethod(HttpClient httpClient, HttpRequest.Builder builder) throws IOException, InterruptedException {
      System.out.println("----------------------------------------");
      System.out.println("Executing POST");
      System.out.println("----------------------------------------");

      HttpRequest request = builder.POST(HttpRequest.BodyPublishers.noBody()).build();
      HttpResponse<Void> response = httpClient.send(request, HttpResponse.BodyHandlers.discarding());

      System.out.println("----------------------------------------");
      System.out.println(response.statusCode());
      System.out.println("----------------------------------------");
   }

   /**
    * putMethod stores a String value in your cache.
    * @param httpClient HTTP client that sends requests and receives responses
    * @param builder    Encapsulates HTTP requests
    * @throws IOException
    * @throws InterruptedException
    */
   public void putMethod(HttpClient httpClient, HttpRequest.Builder builder) throws IOException, InterruptedException {
      System.out.println("----------------------------------------");
      System.out.println("Executing PUT");
      System.out.println("----------------------------------------");

      String cacheValue = "Infinispan REST Test";
      HttpRequest request = builder.PUT(HttpRequest.BodyPublishers.ofString(cacheValue)).build();
      HttpResponse<Void> response = httpClient.send(request, HttpResponse.BodyHandlers.discarding());

      System.out.println("----------------------------------------");
      System.out.println(response.statusCode());
      System.out.println("----------------------------------------");
   }

   /**
    * getMethod get a String value from your cache.
    * @param httpClient HTTP client that sends requests and receives responses
    * @param builder    Encapsulates HTTP requests
    * @return           String value
    * @throws IOException
    */
   public String getMethod(HttpClient httpClient, HttpRequest.Builder builder) throws IOException, InterruptedException {
      System.out.println("----------------------------------------");
      System.out.println("Executing GET");
      System.out.println("----------------------------------------");

      HttpRequest request = builder.GET().build();
      HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

      System.out.println("Executing get method of value: " + response.body());

      System.out.println("----------------------------------------");
      System.out.println(response.statusCode());
      System.out.println("----------------------------------------");

      return response.body();
   }

   public static void main(String[] args) throws IOException, InterruptedException {
      RestExample restExample = new RestExample();
      HttpClient httpClient = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();

      restExample.postMethod(httpClient, getHttpReqestBuilder(String.format("%s%s", SERVER_ADDRESS, CACHE_URI)));
      restExample.putMethod(httpClient, getHttpReqestBuilder(String.format("%s%s/1", SERVER_ADDRESS, CACHE_URI)));
      restExample.getMethod(httpClient, getHttpReqestBuilder(String.format("%s%s/1", SERVER_ADDRESS, CACHE_URI)));
   }

   private static String basicAuth(String username, String password) {
      return "Basic " + Base64.getEncoder().encodeToString((username + ":" + password).getBytes());
   }

   private static final HttpRequest.Builder getHttpReqestBuilder(String url) {
      return HttpRequest.newBuilder()
            .uri(URI.create(url))
            .header("Content-Type", "text/plain")
            .header("Authorization", basicAuth("user", "pass"));
   }
}
