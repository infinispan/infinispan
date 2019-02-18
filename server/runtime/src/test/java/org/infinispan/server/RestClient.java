package org.infinispan.server;

import java.io.IOException;

import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.infinispan.commons.api.BasicCacheContainer;

public class RestClient {

   private final HttpClient client = HttpClientBuilder.create().build();
   // using URI won't do any good since we need to concatenate and there is no method to do this
   private final String baseUri;
   private String cache = BasicCacheContainer.DEFAULT_CACHE_NAME;

   public RestClient(String uri) {
      this.baseUri = uri;
   }

   public RestClient cache(String cache) {
      this.cache = cache;
      return this;
   }

   public String get(String key) {
      try {
         HttpGet getRequest = new HttpGet(constructPath(baseUri, cache, key));
         return EntityUtils.toString(client.execute(getRequest).getEntity());
      } catch (IOException e) {
         throw new AssertionError(e);
      }
   }

   public void put(String key, String value) {
      try {
         HttpPost putRequest = new HttpPost(constructPath(baseUri, cache, key));
         putRequest.setEntity(new StringEntity(value));
         EntityUtils.consume(client.execute(putRequest).getEntity());
      } catch (Exception e) {
         throw new AssertionError(e);
      }
   }

   String constructPath(String baseUri, String cache, String key) {
      return baseUri + "/" + cache + "/" + key;
   }
}
