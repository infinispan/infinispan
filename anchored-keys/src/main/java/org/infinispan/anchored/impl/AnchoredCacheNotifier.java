package org.infinispan.anchored.impl;

import java.util.concurrent.CompletionStage;

import org.infinispan.notifications.cachelistener.CacheNotifierImpl;

/**
 * Skip notifications for entries that only store a location.
 *
 * @author Dan Berindei
 * @since 11
 */
public class AnchoredCacheNotifier<K, V> extends CacheNotifierImpl<K, V> {
   @Override
   public CompletionStage<Void> addListenerAsync(Object listener) {
      return super.addListenerAsync(listener);
   }
}
