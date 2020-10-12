package org.infinispan.anchored.impl;

import java.util.concurrent.CompletionStage;

import org.infinispan.notifications.cachelistener.CacheNotifierImpl;

/**
 * Adjust notifications for anchored keys caches.
 *
 * <ul>
 *    <li>Invoke clustered listeners from the primary owner</li>
 *    <li>Skip notifications for entries that only store a location.</li>
 * </ul>
 *
 *
 * @author Dan Berindei
 * @since 11
 */
public class AnchoredCacheNotifier<K, V> extends CacheNotifierImpl<K, V> {
   @Override
   protected boolean clusterListenerOnPrimaryOnly() {
      return true;
   }

   @Override
   public CompletionStage<Void> addListenerAsync(Object listener) {
      // TODO Skip notification if value is null and metadata is a RemoteMetadata (see ISPN-12289)
      return super.addListenerAsync(listener);
   }
}
