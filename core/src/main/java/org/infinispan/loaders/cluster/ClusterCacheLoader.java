package org.infinispan.loaders.cluster;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.configuration.cache.CacheLoaderConfiguration;
import org.infinispan.configuration.cache.ClusterCacheLoaderConfiguration;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.spi.AbstractCacheLoader;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.remoting.responses.ClusteredGetResponseValidityFilter;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Cache loader that consults other members in the cluster for values. A <code>remoteCallTimeout</code> property is
 * required, a <code>long</code> that specifies in milliseconds how long to wait for results before returning a null.
 *
 * @author Mircea.Markus@jboss.com
 */
public class ClusterCacheLoader extends AbstractCacheLoader {
   private static final Log log = LogFactory.getLog(ClusterCacheLoader.class);

   private RpcManager rpcManager;
   private AdvancedCache<?, ?> cache;

   private ClusterCacheLoaderConfiguration configuration;

   @Override
   public void init(CacheLoaderConfiguration configuration, Cache<?, ?> cache,
                    StreamingMarshaller m) throws CacheLoaderException {

      this.configuration = validateConfigurationClass(configuration, ClusterCacheLoaderConfiguration.class);
      super.init(configuration, cache, m);
      this.cache = cache.getAdvancedCache();
      rpcManager = this.cache.getRpcManager();
   }

   @Override
   public InternalCacheEntry load(Object key) throws CacheLoaderException {
      if (!(isCacheReady() && isLocalCall())) return null;

      ClusteredGetCommand clusteredGetCommand = new ClusteredGetCommand(
            key, cache.getName(), InfinispanCollections.<Flag>emptySet(), false, null,
            cache.getCacheConfiguration().dataContainer().keyEquivalence());

      Collection<Response> responses = doRemoteCall(clusteredGetCommand);
      if (responses.isEmpty()) return null;

      Response response;
      if (responses.size() > 1) {
         // Remove duplicates before deciding if multiple responses were received
         Set<Response> setResponses = new HashSet<Response>(responses);
         if (setResponses.size() > 1)
            throw new CacheLoaderException(String.format(
                  "Responses contains more than 1 element and these elements are not equal, so can't decide which one to use: %s",
                  setResponses));
         response = setResponses.iterator().next();
      } else {
         response = responses.iterator().next();
      }

      if (response.isSuccessful() && response instanceof SuccessfulResponse) {
         InternalCacheValue value = (InternalCacheValue) ((SuccessfulResponse) response).getResponseValue();
         return value.toInternalCacheEntry(key);
      }

      log.unknownResponsesFromRemoteCache(responses);
      throw new CacheLoaderException("Unknown responses");
   }

   @Override
   @SuppressWarnings(value = "unchecked")
   public Set<InternalCacheEntry> loadAll() throws CacheLoaderException {
      return InfinispanCollections.emptySet();
   }

   @Override
   public Set<InternalCacheEntry> load(int maxElems) throws CacheLoaderException {
      return InfinispanCollections.emptySet();
   }

   @Override
   public Set<Object> loadAllKeys(Set<Object> keysToExclude) throws CacheLoaderException {
      return InfinispanCollections.emptySet();
   }

   @Override
   public void start() throws CacheLoaderException {
      //nothing to do here
   }

   @Override
   public void stop() throws CacheLoaderException {
      //nothing to do here
   }

   private Collection<Response> doRemoteCall(ClusteredGetCommand clusteredGetCommand) throws CacheLoaderException {
      Set<Address> members = new HashSet<Address>(rpcManager.getTransport().getMembers());
      Address self = rpcManager.getTransport().getAddress();
      ResponseFilter filter = new ClusteredGetResponseValidityFilter(members, self);
      try {
         RpcOptions options = rpcManager.getRpcOptionsBuilder(ResponseMode.WAIT_FOR_VALID_RESPONSE)
               .timeout(configuration.remoteCallTimeout(), TimeUnit.MILLISECONDS).responseFilter(filter).build();
         return rpcManager.invokeRemotely(null, clusteredGetCommand, options).values();
      } catch (Exception e) {
         log.errorDoingRemoteCall(e);
         throw new CacheLoaderException(e);
      }
   }

   private boolean isLocalCall() {
      InvocationContext invocationContext = cache.getInvocationContextContainer().getInvocationContext(false);
      return invocationContext.isOriginLocal();
   }

   /**
    * A test to check whether the cache is in its started state.  If not, calls should not be made as the channel may
    * not have properly started, blocks due to state transfers may be in progress, etc.
    *
    * @return true if the cache is in its STARTED state.
    */
   protected boolean isCacheReady() {
      return cache.getStatus() == ComponentStatus.RUNNING;
   }
}
