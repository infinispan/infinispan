package org.infinispan.notifications.cachelistener.cluster;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.distexec.DefaultExecutorService;
import org.infinispan.distexec.DistributedCallable;
import org.infinispan.distexec.DistributedExecutorService;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.Ids;
import org.infinispan.filter.Converter;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;
import java.util.UUID;

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

   private transient EmbeddedCacheManager cacheManager;
   private transient CacheNotifier cacheNotifier;
   private transient CacheManagerNotifier cacheManagerNotifier;
   private transient DistributedExecutorService distExecutor;
   private transient Address ourAddress;

   private final UUID identifier;
   private final KeyValueFilter<K, V> filter;
   private final Converter<K, V, ?> converter;
   private final Address origin;

   public ClusterListenerReplicateCallable(UUID identifier, Address origin, KeyValueFilter<K, V> filter,
                                           Converter<K, V, ?> converter) {
      this.identifier = identifier;
      this.origin = origin;
      this.filter = filter;
      this.converter = converter;
   }

   @Override
   public void setEnvironment(Cache<K, V> cache, Set<K> inputKeys) {
      cacheManager = cache.getCacheManager();
      cacheNotifier = cache.getAdvancedCache().getComponentRegistry().getComponent(CacheNotifier.class);
      cacheManagerNotifier = cache.getCacheManager().getGlobalComponentRegistry().getComponent(
            CacheManagerNotifier.class);
      distExecutor = new DefaultExecutorService(cache, new WithinThreadExecutor());
      ourAddress = cache.getCacheManager().getAddress();
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
                        identifier.equals(((RemoteClusterListener)installedListener).getId())) {
                     alreadyInstalled = true;
                     break;
                  }
               }
               if (!alreadyInstalled) {
                  RemoteClusterListener listener = new RemoteClusterListener(identifier, origin, distExecutor, cacheNotifier,
                                                                           cacheManagerNotifier);
                  cacheNotifier.addListener(listener, filter, converter);
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
                  log.tracef("Local cluster listener from origin %s with id % was already installed, ignoring",
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
      return null;
   }

   public static class Externalizer extends AbstractExternalizer<ClusterListenerReplicateCallable> {
      @Override
      public Set<Class<? extends ClusterListenerReplicateCallable>> getTypeClasses() {
         return Util.<Class<? extends ClusterListenerReplicateCallable>>asSet(ClusterListenerReplicateCallable.class);
      }

      @Override
      public void writeObject(ObjectOutput output, ClusterListenerReplicateCallable object) throws IOException {
         output.writeObject(object.identifier);
         output.writeObject(object.origin);
         output.writeObject(object.filter);
         output.writeObject(object.converter);
      }

      @Override
      public ClusterListenerReplicateCallable readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new ClusterListenerReplicateCallable((UUID)input.readObject(), (Address)input.readObject(),
                                                     (KeyValueFilter)input.readObject(), (Converter)input.readObject());
      }

      @Override
      public Integer getId() {
         return Ids.CLUSTER_LISTENER_REPLICATE_CALLABLE;
      }
   }
}
