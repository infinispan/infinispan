package org.infinispan.anchored.impl;

import java.util.function.Function;

import org.infinispan.commands.functional.functions.InjectableComponent;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.RemoteMetadata;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Marshallable function that replaces entry values with location metadata for anchored keys state transfer.
 * The local address is obtained from the RpcManager on each node where this function executes.
 *
 * @author William Burns
 * @since 16.2
 */
@ProtoTypeId(ProtoStreamTypeIds.ANCHORED_REPLACE_WITH_LOCATION_FUNCTION)
@Scope(Scopes.NAMED_CACHE)
public class ReplaceWithLocationFunction<K, V> implements Function<Publisher<CacheEntry<K, V>>, Publisher<CacheEntry<K, V>>>,
                                                           InjectableComponent {
   private static final Log log = LogFactory.getLog(ReplaceWithLocationFunction.class);

   @Inject
   transient InternalEntryFactory internalEntryFactory;

   @Inject
   transient RpcManager rpcManager;

   @ProtoFactory
   public ReplaceWithLocationFunction() {
   }

   @Override
   public void inject(ComponentRegistry registry) {
      internalEntryFactory = registry.getInternalEntryFactory().running();
      rpcManager = registry.getRpcManager().running();
   }

   @Override
   public Publisher<CacheEntry<K, V>> apply(Publisher<CacheEntry<K, V>> publisher) {
      return Flowable.fromPublisher(publisher)
            .map(this::replaceValueWithLocation);
   }

   private CacheEntry<K, V> replaceValueWithLocation(CacheEntry<K, V> entry) {
      if (entry.getMetadata() instanceof RemoteMetadata) {
         return entry;
      } else {
         if (log.isTraceEnabled()) {
            log.tracef("Replaced state transfer value for key %s: %s", entry.getKey(), rpcManager.getAddress());
         }
         RemoteMetadata metadata = new RemoteMetadata(rpcManager.getAddress(), null);
         return internalEntryFactory.create(entry.getKey(), null, metadata);
      }
   }
}
