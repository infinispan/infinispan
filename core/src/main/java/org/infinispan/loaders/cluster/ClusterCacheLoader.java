package org.infinispan.loaders.cluster;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.context.InvocationContext;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.loaders.AbstractCacheLoader;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderMetadata;
import org.infinispan.marshall.Marshaller;
import org.infinispan.remoting.responses.ClusteredGetResponseValidityFilter;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Cache loader that consults other members in the cluster for values. A <code>timeout</code> property is required, a
 * <code>long</code> that specifies in milliseconds how long to wait for results before returning a null.
 *
 * @author Mircea.Markus@jboss.com
 */
@CacheLoaderMetadata(configurationClass = ClusterCacheLoaderConfig.class)
public class ClusterCacheLoader extends AbstractCacheLoader {
   private static Log log = LogFactory.getLog(ClusterCacheLoader.class);

   private ClusterCacheLoaderConfig config;
   private RpcManager rpcManager;
   private AdvancedCache cache;

   public void init(CacheLoaderConfig config, Cache cache, Marshaller m) {
      this.config = (ClusterCacheLoaderConfig) config;
      this.cache = cache.getAdvancedCache();
      rpcManager = this.cache.getRpcManager();
   }

   public InternalCacheEntry load(Object key) throws CacheLoaderException {
      if (!(isCacheReady() && isLocalCall())) return null;
      ClusteredGetCommand clusteredGetCommand = new ClusteredGetCommand(key, cache.getName());
      List<Response> response = doRemoteCall(clusteredGetCommand);
      if (response.isEmpty()) return null;
      if (response.size() > 1)
         throw new CacheLoaderException("Response length is always 0 or 1, received: " + response);
      Response firstResponse = response.get(0);
      if (firstResponse.isSuccessful() && firstResponse instanceof SuccessfulResponse) {
         InternalCacheValue value = (InternalCacheValue) ((SuccessfulResponse) firstResponse).getResponseValue();
         return value.toInternalCacheEntry(key);
      }
      String message = "Unknown response from remote cache: " + response;
      log.error(message);
      throw new CacheLoaderException(message);
   }

   @SuppressWarnings(value = "unchecked")
   public Set<InternalCacheEntry> loadAll() throws CacheLoaderException {
      return Collections.emptySet();
   }

   public void start() throws CacheLoaderException {
      //nothing to do here
   }

   public void stop() throws CacheLoaderException {
      //nothing to do here
   }

   public Class<? extends CacheLoaderConfig> getConfigurationClass() {
      return ClusterCacheLoaderConfig.class;
   }

   private List<Response> doRemoteCall(ClusteredGetCommand clusteredGetCommand) throws CacheLoaderException {
      Set<Address> validMembers = new HashSet<Address>(rpcManager.getTransport().getMembers());
      validMembers.remove(rpcManager.getTransport().getAddress());
      ResponseFilter filter = new ClusteredGetResponseValidityFilter(validMembers);
      try {
         return rpcManager.invokeRemotely(null, clusteredGetCommand, ResponseMode.WAIT_FOR_VALID_RESPONSE, config.getRemoteCallTimeout(), false, filter);
      } catch (Exception e) {
         log.error("error while doing remote call", e);
         throw new CacheLoaderException(e);
      }
   }

   private boolean isLocalCall() {
      InvocationContext invocationContext = cache.getInvocationContextContainer().getInvocationContext();
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
