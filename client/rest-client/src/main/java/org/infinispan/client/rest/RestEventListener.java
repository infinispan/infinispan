package org.infinispan.client.rest;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 13.0
 **/
public interface RestEventListener {
   default void onOpen(RestResponse response) {}

   default void onError(Throwable t, RestResponse response) {}

   default void onMessage(String id, String type, String data) {}

   default void close() {}
}
