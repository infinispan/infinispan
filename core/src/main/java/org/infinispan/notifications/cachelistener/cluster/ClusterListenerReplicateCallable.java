package org.infinispan.notifications.cachelistener.cluster;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.annotation.Annotation;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.distexec.DistributedCallable;
import org.infinispan.distexec.DistributedExecutorService;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.Ids;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachelistener.ListenerHolder;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterConverter;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.remoting.transport.Address;
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
public class ClusterListenerReplicateCallable<K, V> implements DistributedCallable<K, V, Void> {
   private static final Log log = LogFactory.getLog(ClusterListenerReplicateCallable.class);
   private static final boolean trace = log.isTraceEnabled();

   private transient EmbeddedCacheManager cacheManager;
   private transient CacheNotifier cacheNotifier;
   private transient CacheManagerNotifier cacheManagerNotifier;
   private transient DistributedExecutorService distExecutor;
   private transient Address ourAddress;
   private transient ClusterEventManager<K, V> eventManager;

   private final UUID identifier;
   private final CacheEventFilter<K, V> filter;
   private final CacheEventConverter<K, V, ?> converter;
   private final Address origin;
   private final boolean sync;
   private final Set<Class<? extends Annotation>> filterAnnotations;
   private final DataConversion keyDataConversion;
   private final DataConversion valueDataConversion;

   public ClusterListenerReplicateCallable(UUID identifier, Address origin, CacheEventFilter<K, V> filter,
                                           CacheEventConverter<K, V, ?> converter, boolean sync,
                                           Set<Class<? extends Annotation>> filterAnnotations,
                                           DataConversion keyDataConversion, DataConversion valueDataConversion) {
      this.identifier = identifier;
      this.origin = origin;
      this.filter = filter;
      this.converter = converter;
      this.sync = sync;
      this.filterAnnotations = filterAnnotations;
      this.keyDataConversion = keyDataConversion;
      this.valueDataConversion = valueDataConversion;

      if (trace)
         log.tracef("Created clustered listener replicate callable for: %s", filterAnnotations);
   }

   @Override
   public void setEnvironment(Cache<K, V> cache, Set<K> inputKeys) {
      cacheManager = cache.getCacheManager();

      ComponentRegistry componentRegistry = cache.getAdvancedCache().getComponentRegistry();

      cacheNotifier = componentRegistry.getComponent(CacheNotifier.class);
      cacheManagerNotifier = cache.getCacheManager().getGlobalComponentRegistry().getComponent(
            CacheManagerNotifier.class);
      distExecutor = SecurityActions.getDefaultExecutorService(cache);
      ourAddress = cache.getCacheManager().getAddress();
      eventManager = componentRegistry.getComponent(ClusterEventManager.class);
      if (filter != null) {
         componentRegistry.wireDependencies(filter);
      }
      if (converter != null && converter != filter) {
         componentRegistry.wireDependencies(converter);
      }
   }

   @Override
   public Void call() throws Exception {
      // Only register listeners if we aren't the ones that registered the cluster listener
      if (!ourAddress.equals(origin)) {
         // Make sure the origin is around otherwise don't register the listener - some way with identifier (CHM maybe?)
         if (cacheManager.getMembers().contains(origin)) {
            // Prevent multiple invocations to get in here at once, which should prevent concurrent registration of
            // the same id.  Note we can't use a static CHM due to running more than 1 node in same JVM
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
                  RemoteClusterListener listener = new RemoteClusterListener(identifier, origin, distExecutor, cacheNotifier,
                        cacheManagerNotifier, eventManager, sync);
                  ListenerHolder listenerHolder = new ListenerHolder(listener, keyDataConversion, valueDataConversion);
                  cacheNotifier.addFilteredListener(listenerHolder, filter, converter, filterAnnotations);
                  cacheManagerNotifier.addListener(listener);
                  // It is possible the member is now gone after registered, if so we have to remove just to be sure
                  if (!cacheManager.getMembers().contains(origin)) {
                     cacheNotifier.removeListener(listener);
                     cacheManagerNotifier.removeListener(listener);
                     if (trace) {
                        log.tracef("Removing local cluster listener for remote cluster listener that was just registered, as the origin %s went away concurrently", origin);
                     }
                  } else if (trace) {
                     log.tracef("Registered local cluster listener for remote cluster listener from origin %s with id %s",
                           origin, identifier);
                  }
               } else if (trace) {
                  log.tracef("Local cluster listener from origin %s with id %s was already installed, ignoring",
                        origin, identifier);
               }
            }
         } else if (trace) {
            log.tracef("Not registering local cluster listener for remote cluster listener from origin %s, as the origin went away",
                  origin);
         }
      } else if (trace) {
         log.trace("Not registering local cluster listener as we are the node who registered the cluster listener");
      }
      return null;
   }

   public static class Externalizer extends AbstractExternalizer<ClusterListenerReplicateCallable> {
      @Override
      public Set<Class<? extends ClusterListenerReplicateCallable>> getTypeClasses() {
         return Collections.singleton(ClusterListenerReplicateCallable.class);
      }

      @Override
      public void writeObject(ObjectOutput output, ClusterListenerReplicateCallable object) throws IOException {
         output.writeObject(object.identifier);
         output.writeObject(object.origin);
         output.writeObject(object.filter);
         if (object.filter == object.converter && object.filter instanceof CacheEventFilterConverter) {
            output.writeBoolean(true);
         } else {
            output.writeBoolean(false);
            output.writeObject(object.converter);
         }
         output.writeBoolean(object.sync);
         MarshallUtil.marshallCollection(object.filterAnnotations, output);
         DataConversion.writeTo(output, object.keyDataConversion);
         DataConversion.writeTo(output, object.valueDataConversion);
      }

      @Override
      public ClusterListenerReplicateCallable readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         UUID id = (UUID) input.readObject();
         Address address = (Address) input.readObject();
         CacheEventFilter filter = (CacheEventFilter) input.readObject();
         boolean sameConverter = input.readBoolean();
         CacheEventConverter converter;
         if (sameConverter) {
            converter = (CacheEventFilterConverter) filter;
         } else {
            converter = (CacheEventConverter) input.readObject();
         }
         boolean sync = input.readBoolean();
         Set<Class<? extends Annotation>> listenerAnnots = MarshallUtil.unmarshallCollection(input, HashSet::new);
         DataConversion keyDataConversion = DataConversion.readFrom(input);
         DataConversion valueDataConversion = DataConversion.readFrom(input);
         return new ClusterListenerReplicateCallable(id, address, filter, converter, sync, listenerAnnots,
               keyDataConversion, valueDataConversion);
      }

      @Override
      public Integer getId() {
         return Ids.CLUSTER_LISTENER_REPLICATE_CALLABLE;
      }
   }
}
