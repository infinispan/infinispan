package org.infinispan.anchored.impl;

import org.infinispan.commons.util.IntSet;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.RemoteMetadata;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.reactive.publisher.impl.Notifications;
import org.infinispan.reactive.publisher.impl.SegmentPublisherSupplier;
import org.infinispan.statetransfer.StateProvider;
import org.infinispan.statetransfer.StateProviderImpl;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import io.reactivex.rxjava3.core.Flowable;

/**
 * State provider that replaces the entry values with the node address.
 *
 * @author Dan Berindei
 * @since 11
 */
@Scope(Scopes.NAMED_CACHE)
public class AnchoredStateProvider extends StateProviderImpl implements StateProvider {
   private static final Log log = LogFactory.getLog(AnchoredStateProvider.class);

   @Inject InternalEntryFactory internalEntryFactory;

   @Override
   protected Flowable<SegmentPublisherSupplier.Notification<InternalCacheEntry<?, ?>>> readEntries(IntSet segments) {
      return super.readEntries(segments)
            .map(notification -> {
               if (notification.isValue()) {
                  InternalCacheEntry<Object, Object> ice = (InternalCacheEntry<Object, Object>) notification.value();
                  return Notifications.value(replaceValueWithLocation(ice), notification.valueSegment());
               }
               return notification;
            });
   }

   private InternalCacheEntry<Object, Object> replaceValueWithLocation(InternalCacheEntry<Object, Object> ice) {
      if (ice.getMetadata() instanceof RemoteMetadata) {
         return ice;
      } else {
         if (log.isTraceEnabled()) log.tracef("Replaced state transfer value for key %s: %s", ice.getKey(), rpcManager.getAddress());
         RemoteMetadata metadata = new RemoteMetadata(rpcManager.getAddress(), null);
         return internalEntryFactory.create(ice.getKey(), null, metadata);
      }
   }
}
