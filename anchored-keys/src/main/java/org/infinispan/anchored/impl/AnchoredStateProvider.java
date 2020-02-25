package org.infinispan.anchored.impl;

import org.infinispan.commons.util.IntSet;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.StateProvider;
import org.infinispan.statetransfer.StateProviderImpl;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import io.reactivex.Flowable;

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
   protected Flowable<InternalCacheEntry<Object, Object>> publishDataContainerEntries(IntSet segments) {
      return super.publishDataContainerEntries(segments)
            .map(ice -> {
               if (ice.getValue() instanceof Address) {
                  return ice;
               } else {
                  InternalCacheEntry<Object, Object> clone = internalEntryFactory.create(ice.getKey(), rpcManager.getAddress(), ice);
                  log.tracef("Replaced state transfer value for key %s: %s", ice.getKey(), rpcManager.getAddress());
                  return clone;
               }
            });
   }

   @Override
   protected Flowable<InternalCacheEntry<Object, Object>> publishStoreEntries(IntSet segments) {
      return super.publishStoreEntries(segments);
   }
}
