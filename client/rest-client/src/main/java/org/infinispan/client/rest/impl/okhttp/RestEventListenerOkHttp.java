package org.infinispan.client.rest.impl.okhttp;

import org.infinispan.client.rest.RestEventListener;

import okhttp3.Response;
import okhttp3.sse.EventSource;
import okhttp3.sse.EventSourceListener;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 13.0
 **/
public class RestEventListenerOkHttp extends EventSourceListener {

   private final RestEventListener listener;

   public RestEventListenerOkHttp(RestEventListener listener) {
      this.listener = listener;
   }

   @Override
   public void onOpen(EventSource eventSource, Response response) {
      listener.onOpen(new RestResponseOkHttp(response));
   }

   @Override
   public void onEvent(EventSource eventSource, String id, String type, String data) {
      listener.onMessage(id, type, data);
   }

   @Override
   public void onClosed(EventSource eventSource) {
      listener.close();
   }

   @Override
   public void onFailure(EventSource eventSource, Throwable t, Response response) {
      listener.onError(t, new RestResponseOkHttp(response));
   }
}
