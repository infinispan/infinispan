package org.horizon.loader.cluster;

import org.horizon.AdvancedCache;
import org.horizon.Cache;
import org.horizon.commands.remote.ClusteredGetCommand;
import org.horizon.container.entries.InternalCacheEntry;
import org.horizon.context.InvocationContext;
import org.horizon.lifecycle.ComponentStatus;
import org.horizon.loader.AbstractCacheLoader;
import org.horizon.loader.CacheLoaderConfig;
import org.horizon.loader.CacheLoaderException;
import org.horizon.logging.Log;
import org.horizon.logging.LogFactory;
import org.horizon.marshall.Marshaller;
import org.horizon.remoting.RPCManager;
import org.horizon.remoting.ResponseFilter;
import org.horizon.remoting.ResponseMode;
import org.horizon.remoting.transport.Address;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 *  Cache loader that consults other members in the cluster for values. A
 * <code>timeout</code> property is required, a <code>long</code> that
 * specifies in milliseconds how long to wait for results before returning a
 * null.
 *
 * @author Mircea.Markus@jboss.com
 */
public class ClusterCacheLoader extends AbstractCacheLoader {

   private static Log log = LogFactory.getLog(ClusterCacheLoader.class);
   private static boolean trace = log.isTraceEnabled();

   private ClusterCacheLoaderConfig config;
   private RPCManager rpcManager;
   private AdvancedCache cache;

   public void init(CacheLoaderConfig config, Cache cache, Marshaller m) {
      this.config = (ClusterCacheLoaderConfig) config;
      this.cache = cache.getAdvancedCache();
      rpcManager = this.cache.getRpcManager();
   }

   public InternalCacheEntry load(Object key) throws CacheLoaderException {
      if (!(isCacheReady() && isLocalCall())) return null;
      ClusteredGetCommand clusteredGetCommand = new ClusteredGetCommand(key, cache.getName());
      List<Object> response = doRemoteCall(clusteredGetCommand);
      if (response.isEmpty()) return null;
      if (response.size() > 1)
         throw new CacheLoaderException("Response length is always 0 or 1, received: " + response);
      Object firstResponse = response.get(0);
      if (firstResponse instanceof InternalCacheEntry)
         return (InternalCacheEntry) firstResponse;
      return (InternalCacheEntry) unknownResponse(firstResponse);
   }

   @SuppressWarnings(value = "unchecked")
   public Set<InternalCacheEntry> loadAll() throws CacheLoaderException {
      return Collections.EMPTY_SET;
   }

   private Object unknownResponse(Object response) throws CacheLoaderException {
      String message = "Unknown response from remote cache: " + response;
      log.error(message);
      throw new CacheLoaderException(message);
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

      public boolean isAcceptable(Object object, Address address) {
         pendingResponders.remove(address);

         if (object instanceof List) {
            List response = (List) object;
            Boolean foundResult = (Boolean) response.get(0);
            if (foundResult) numValidResponses++;
         }
         // always return true to make sure a response is logged by the JGroups RpcDispatcher.
         return true;
      }

      public boolean needMoreResponses() {
         return numValidResponses < 1 && pendingResponders.size() > 0;
      }

   }

   private List<Object> doRemoteCall(ClusteredGetCommand clusteredGetCommand) throws CacheLoaderException {
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
