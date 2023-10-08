package org.infinispan.notifications.cachelistener.cluster;

import java.lang.annotation.Annotation;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachelistener.ListenerHolder;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * This DistributedCallable is used to install a {@link RemoteClusterListener} on the resulting node.  This class
 * also has checks to ensure that if the listener is attempted to be installed from more than 1 source only 1 will be
 * installed as well if a node goes down while installing will also remove the listener.
 *
 * @author wburns
 * @since 7.0
 */
@ProtoTypeId(ProtoStreamTypeIds.CLUSTER_LISTENER_REPLICATE_CALLABLE)
public class ClusterListenerReplicateCallable<K, V> implements Function<EmbeddedCacheManager, Void>,
      BiConsumer<EmbeddedCacheManager, Cache<K, V>> {
   private static final Log log = LogFactory.getLog(ClusterListenerReplicateCallable.class);

   private final CacheEventFilter<K, V> filter;
   private final CacheEventConverter<K, V, ?> converter;

   @ProtoField(1)
   final UUID identifier;

   @ProtoField(2)
   final String cacheName;

   @ProtoField(number = 3, javaType = JGroupsAddress.class)
   final Address origin;

   @ProtoField(number = 4, defaultValue = "false")
   final boolean sync;

   @ProtoField(number = 5, collectionImplementation = HashSet.class)
   final Set<Class<? extends Annotation>> filterAnnotations;

   @ProtoField(number = 6)
   final DataConversion keyDataConversion;

   @ProtoField(number = 7)
   final DataConversion valueDataConversion;

   @ProtoField(number = 8, defaultValue = "false")
   final boolean useStorageFormat;

   public ClusterListenerReplicateCallable(String cacheName, UUID identifier, Address origin, CacheEventFilter<K, V> filter,
                                           CacheEventConverter<K, V, ?> converter, boolean sync,
                                           Set<Class<? extends Annotation>> filterAnnotations,
                                           DataConversion keyDataConversion, DataConversion valueDataConversion, boolean useStorageFormat) {
      this.cacheName = cacheName;
      this.identifier = identifier;
      this.origin = origin;
      this.filter = filter;
      this.converter = converter;
      this.sync = sync;
      this.filterAnnotations = filterAnnotations;
      this.keyDataConversion = keyDataConversion;
      this.valueDataConversion = valueDataConversion;
      this.useStorageFormat = useStorageFormat;

      if (log.isTraceEnabled())
         log.tracef("Created clustered listener replicate callable for: %s", filterAnnotations);
   }

   @ProtoFactory
   ClusterListenerReplicateCallable(UUID identifier, String cacheName, JGroupsAddress origin, boolean sync,
                                    Set<Class<? extends Annotation>> filterAnnotations, DataConversion keyDataConversion,
                                    DataConversion valueDataConversion, boolean useStorageFormat,
                                    MarshallableObject<CacheEventFilter<K, V>> filter,
                                    MarshallableObject<CacheEventConverter<K, V, ?>> converter) {
      this(cacheName, identifier, origin, MarshallableObject.unwrap(filter), MarshallableObject.unwrap(converter), sync,
            filterAnnotations, keyDataConversion, valueDataConversion, useStorageFormat);
   }

   @ProtoField(number = 9)
   MarshallableObject<CacheEventFilter<K, V>> getFilter() {
      return MarshallableObject.create(filter);
   }

   @ProtoField(number = 10)
   MarshallableObject<CacheEventConverter<K, V, ?>> getConverter() {
      return MarshallableObject.create(converter);
   }

   @Override
   public Void apply(EmbeddedCacheManager cacheManager) {
      Cache<K, V> cache = SecurityActions.getCache(cacheManager, cacheName);
      accept(cacheManager, cache);
      return null;
   }

   @Override
   public void accept(EmbeddedCacheManager cacheManager, Cache<K, V> cache) {
      ComponentRegistry componentRegistry = SecurityActions.getCacheComponentRegistry(cache.getAdvancedCache());
      CacheNotifier<K, V> cacheNotifier = componentRegistry.getComponent(CacheNotifier.class);
      CacheManagerNotifier cacheManagerNotifier = componentRegistry.getComponent(CacheManagerNotifier.class);
      Address ourAddress = cache.getCacheManager().getAddress();
      ClusterEventManager<K, V> eventManager = componentRegistry.getComponent(ClusterEventManager.class);
      if (filter != null) {
         componentRegistry.wireDependencies(filter);
      }
      if (converter != null && converter != filter) {
         componentRegistry.wireDependencies(converter);
      }

      // Only register listeners if we aren't the ones that registered the cluster listener
      if (!ourAddress.equals(origin)) {
         // Make sure the origin is around otherwise don't register the listener - some way with identifier (CHM maybe?)
         if (cacheManager.getMembers().contains(origin)) {
            // Prevent multiple invocations to get in here at once, which should prevent concurrent registration of
            // the same id.  Note we can't use a static CHM due to running more than 1 node in same JVM
            // TODO virtual thread pinning, waiting on CF while inside the synchronized block
            synchronized (cacheNotifier) {
               boolean alreadyInstalled = false;
               // First make sure the listener is not already installed, if it is we don't do anything.
               for (Object installedListener : cacheNotifier.getListeners()) {
                  if (installedListener instanceof RemoteClusterListener &&
                        identifier.equals(((RemoteClusterListener) installedListener).getId())) {
                     alreadyInstalled = true;
                     break;
                  }
               }
               if (!alreadyInstalled) {
                  RemoteClusterListener listener = new RemoteClusterListener(identifier, origin, cacheNotifier,
                        cacheManagerNotifier, eventManager, sync);
                  ListenerHolder listenerHolder = new ListenerHolder(listener, keyDataConversion, valueDataConversion, useStorageFormat);
                  cacheNotifier.addFilteredListener(listenerHolder, filter, converter, filterAnnotations);
                  cacheManagerNotifier.addListener(listener);
                  // It is possible the member is now gone after registered, if so we have to remove just to be sure
                  if (!cacheManager.getMembers().contains(origin)) {
                     cacheNotifier.removeListener(listener);
                     cacheManagerNotifier.removeListener(listener);
                     if (log.isTraceEnabled()) {
                        log.tracef("Removing local cluster listener for remote cluster listener that was just registered, as the origin %s went away concurrently", origin);
                     }
                  } else if (log.isTraceEnabled()) {
                     log.tracef("Registered local cluster listener for remote cluster listener from origin %s with id %s",
                           origin, identifier);
                  }
               } else if (log.isTraceEnabled()) {
                  log.tracef("Local cluster listener from origin %s with id %s was already installed, ignoring",
                        origin, identifier);
               }
            }
         } else if (log.isTraceEnabled()) {
            log.tracef("Not registering local cluster listener for remote cluster listener from origin %s, as the origin went away",
                  origin);
         }
      } else if (log.isTraceEnabled()) {
         log.trace("Not registering local cluster listener as we are the node who registered the cluster listener");
      }
   }

   @Override
   public String toString() {
      return "ClusterListenerReplicateCallable{" +
            "cacheName='" + cacheName + '\'' +
            ", identifier=" + identifier +
            ", origin=" + origin +
            ", sync=" + sync +
            '}';
   }
}
