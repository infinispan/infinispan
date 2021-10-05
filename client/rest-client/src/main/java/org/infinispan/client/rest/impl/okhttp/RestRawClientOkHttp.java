package org.infinispan.client.rest.impl.okhttp;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.client.rest.RestEventListener;
import org.infinispan.client.rest.RestRawClient;
import org.infinispan.client.rest.RestResponse;

import okhttp3.FormBody;
import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.internal.Util;
import okhttp3.sse.EventSource;
import okhttp3.sse.EventSources;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class RestRawClientOkHttp implements RestRawClient {
   private final RestClientOkHttp restClient;

   RestRawClientOkHttp(RestClientOkHttp restClient) {
      this.restClient = restClient;
   }

   @Override
   public CompletionStage<RestResponse> postForm(String url, Map<String, String> headers, Map<String, List<String>> formParameters) {
      Request.Builder builder = new Request.Builder();
      builder.url(restClient.getBaseURL() + url);
      headers.forEach(builder::header);
      FormBody.Builder form = new FormBody.Builder();
      formParameters.forEach((k, vs) -> vs.forEach(v -> form.add(k, v)));
      builder.post(form.build());
      return restClient.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> post(String url, String body, String bodyMediaType) {
      Request.Builder builder = new Request.Builder();
      builder.url(restClient.getBaseURL() + url);
      builder.post(RequestBody.create(MediaType.parse(bodyMediaType), body));
      return restClient.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> post(String url, Map<String, String> headers) {
      Request.Builder builder = new Request.Builder();
      builder.url(restClient.getBaseURL() + url);
      headers.forEach(builder::header);
      builder.post(Util.EMPTY_REQUEST);
      return restClient.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> post(String url, Map<String, String> headers, String body, String bodyMediaType) {
      Request.Builder builder = new Request.Builder();
      builder.url(restClient.getBaseURL() + url);
      headers.forEach(builder::header);
      builder.post(RequestBody.create(MediaType.parse(bodyMediaType), body));
      return restClient.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> putValue(String url, Map<String, String> headers, String body, String bodyMediaType) {
      Request.Builder builder = new Request.Builder();
      builder.url(restClient.getBaseURL() + url);
      headers.forEach(builder::header);
      builder.put(RequestBody.create(MediaType.parse(bodyMediaType), body));
      return restClient.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> put(String url, Map<String, String> headers) {
      Request.Builder builder = new Request.Builder();
      builder.url(restClient.getBaseURL() + url);
      headers.forEach(builder::header);
      builder.put(Util.EMPTY_REQUEST);
      return restClient.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> get(String url, Map<String, String> headers) {
      Request.Builder builder = new Request.Builder().get().url(restClient.getBaseURL() + url);
      headers.forEach(builder::header);
      return restClient.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> delete(String url, Map<String, String> headers) {
      Request.Builder builder = new Request.Builder();
      builder.url(restClient.getBaseURL() + url);
      headers.forEach(builder::header);
      builder.delete();
      return restClient.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> options(String url, Map<String, String> headers) {
      Request.Builder builder = new Request.Builder();
      builder.url(restClient.getBaseURL() + url);
      headers.forEach(builder::header);
      builder.method("OPTIONS", Util.EMPTY_REQUEST);
      return restClient.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> head(String url, Map<String, String> headers) {
      Request.Builder builder = new Request.Builder();
      builder.url(restClient.getBaseURL() + url);
      headers.forEach(builder::header);
      builder.head();
      return restClient.execute(builder);
   }

   @Override
   public Closeable listen(String url, Map<String, String> headers, RestEventListener listener) {
      Request.Builder builder = new Request.Builder();
      builder.url(restClient.getBaseURL() + url);
      headers.forEach(builder::header);
      EventSource.Factory factory = EventSources.createFactory(restClient.client());
      EventSource eventSource = factory.newEventSource(builder.build(), new RestEventListenerOkHttp(listener));
      return () -> eventSource.cancel();
   }
}
