package org.infinispan.client.rest.impl.okhttp;

import static org.infinispan.client.rest.impl.okhttp.RestClientOkHttp.sanitize;

import java.util.concurrent.CompletionStage;

import org.infinispan.client.rest.RestCounterClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;

import okhttp3.Request;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class RestCounterClientOkHttp implements RestCounterClient {
   private final RestClientOkHttp client;
   private final String name;
   private final String counterUrl;

   RestCounterClientOkHttp(RestClientOkHttp client, String name) {
      this.client = client;
      this.name = name;
      this.counterUrl = String.format("%s%s/v2/counters/%s", client.getBaseURL(), client.getConfiguration().contextPath(), sanitize(name)).replaceAll("//", "/");
   }

   @Override
   public CompletionStage<RestResponse> create(RestEntity configuration) {
      Request.Builder builder = new Request.Builder();
      builder.url(counterUrl).post(((RestEntityAdaptorOkHttp) configuration).toRequestBody());
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> delete() {
      Request.Builder builder = new Request.Builder();
      builder.url(counterUrl).delete();
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> configuration() {
      Request.Builder builder = new Request.Builder();
      builder.url(counterUrl + "/config");
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> get() {
      Request.Builder builder = new Request.Builder();
      builder.url(counterUrl);
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> add(long delta) {
      Request.Builder builder = new Request.Builder();
      builder.url(counterUrl + "?action=add&delta=" + delta);
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> increment() {
      Request.Builder builder = new Request.Builder();
      builder.url(counterUrl + "?action=increment");
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> decrement() {
      Request.Builder builder = new Request.Builder();
      builder.url(counterUrl + "?action=decrement");
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> reset() {
      Request.Builder builder = new Request.Builder();
      builder.url(counterUrl + "?action=reset");
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> compareAndSet(long expect, long value) {
      Request.Builder builder = new Request.Builder();
      builder.url(counterUrl + "?action=compareAndSet&expect=" + expect + "&update=" + value);
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> compareAndSwap(long expect, long value) {
      Request.Builder builder = new Request.Builder();
      builder.url(counterUrl + "?action=compareAndSet&expect=" + expect + "&update=" + value);
      return client.execute(builder);
   }
}
