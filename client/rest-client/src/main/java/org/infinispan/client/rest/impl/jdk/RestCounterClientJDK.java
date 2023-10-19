package org.infinispan.client.rest.impl.jdk;

import static org.infinispan.client.rest.impl.jdk.RestClientJDK.ACCEPT;
import static org.infinispan.client.rest.impl.jdk.RestClientJDK.sanitize;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.client.rest.RestCounterClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;

/**
 * @since 14.0
 **/
public class RestCounterClientJDK implements RestCounterClient {
   private final RestRawClientJDK client;
   private final String path;

   RestCounterClientJDK(RestRawClientJDK client, String name) {
      this.client = client;
      this.path = client.getConfiguration().contextPath() + "/v2/counters/" + sanitize(name);
   }

   @Override
   public CompletionStage<RestResponse> create(RestEntity configuration) {
      return client.post(path, configuration);
   }

   @Override
   public CompletionStage<RestResponse> delete() {
      return client.delete(path);
   }

   @Override
   public CompletionStage<RestResponse> configuration() {
      return configuration(null);
   }

   @Override
   public CompletionStage<RestResponse> configuration(String mediaType) {
      return client.get(path+"/config", mediaType != null ? Map.of(ACCEPT, mediaType) : Collections.emptyMap());
   }

   @Override
   public CompletionStage<RestResponse> get() {
      return client.get(path);
   }

   @Override
   public CompletionStage<RestResponse> add(long delta) {
      return client.post(path + "?action=add&delta=" + delta);
   }

   @Override
   public CompletionStage<RestResponse> increment() {
      return client.post(path + "?action=increment");
   }

   @Override
   public CompletionStage<RestResponse> decrement() {
      return client.post(path + "?action=decrement");
   }

   @Override
   public CompletionStage<RestResponse> reset() {
      return client.post(path + "?action=reset");
   }

   @Override
   public CompletionStage<RestResponse> compareAndSet(long expect, long value) {
      return client.post(path + "?action=compareAndSet&expect=" + expect + "&update=" + value);
   }

   @Override
   public CompletionStage<RestResponse> compareAndSwap(long expect, long value) {
      return client.post(path + "?action=compareAndSwap&expect=" + expect + "&update=" + value);
   }

   @Override
   public CompletionStage<RestResponse> getAndSet(long newValue) {
      return client.post(path + "?action=getAndSet&value=" + newValue);
   }
}
