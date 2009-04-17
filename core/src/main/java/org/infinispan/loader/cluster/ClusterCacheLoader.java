package org.infinispan.loader.cluster;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.loader.AbstractCacheLoader;
import org.infinispan.loader.CacheLoaderConfig;
import org.infinispan.loader.CacheLoaderException;
import org.infinispan.logging.Log;
import org.infinispan.logging.LogFactory;
import org.infinispan.marshall.Marshaller;
import org.infinispan.remoting.ResponseFilter;
import org.infinispan.remoting.ResponseMode;
import org.infinispan.remoting.RpcManager;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.transport.Address;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Cache loader that consults other members in the cluster for values. A <code>timeout</code> property is required, a
 * <code>long</code> that specifies in milliseconds how long to wait for results before returning a null.
 *
 * @author Mircea.Markus@jboss.com
 */
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
         return (InternalCacheEntry) ((SuccessfulResponse) firstResponse).getResponseValue();
      }

      String message = "Unknown response from remote cache: " + response;
      log.error(message);
      throw new CacheLoaderException(message);
   }

   @SuppressWarnings(value = "unchecked")
   public Set<InternalCacheEntry> loadAll() throws CacheLoaderException {
      return Collections.EMPTY_SET;
   }

   public void start() throws CacheLoaderException {
      //nothing to do here
   }

   public void stop() throws CacheLoaderException {
      //nothing to do here
   }

   public Class<? extends CacheLoaderConfig> getConfigurationClass() {
      throw new IllegalStateException("TODO - please implement me!!!");
   }

   public static class ResponseValidityFilter implements ResponseFilter {

      private int numValidResponses = 0;

      private List<Address> pendingResponders;

      public ResponseValidityFilter(List<Address> expected, Address localAddress) {
         this.pendingResponders = new ArrayList<Address>(expected);
         // We'll never get a response from ourself
         this.pendingResponders.remove(localAddress);
      }

      public boolean isAcceptable(Response response, Address address) {
         pendingResponders.remove(address);

         if (response instanceof SuccessfulResponse) numValidResponses++;

         // always return true to make sure a response is logged by the JGroups RpcDispatcher.
         return true;
      }

      public boolean needMoreResponses() {
         return numValidResponses < 1 && pendingResponders.size() > 0;
      }

   }

   private List<Response> doRemoteCall(ClusteredGetCommand clusteredGetCommand) throws CacheLoaderException {
      ResponseValidityFilter filter = new ResponseValidityFilter(rpcManager.getTransport().getMembers(), rpcManager.getLocalAddress());
      try {
         return rpcManager.invokeRemotely(null, clusteredGetCommand, ResponseMode.WAIT_FOR_VALID_RESPONSE, config.getRemoteCallTimeout(), false, filter, false);
      } catch (Exception e) {
         log.error("error while doing remote call", e);
         throw new CacheLoaderException(e);
      }
   }

   private boolean isLocalCall() {
      InvocationContext invocationContext = cache.getInvocationContextContainer().get();
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
