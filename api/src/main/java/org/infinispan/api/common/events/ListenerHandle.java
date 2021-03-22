package org.infinispan.api.common.events;

/**
 * @since 14.0
 **/
public interface ListenerHandle<T> extends AutoCloseable {
   T get();

   @Override
   void close();
}
