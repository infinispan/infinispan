package org.infinispan.distribution;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.config.Configuration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.responses.ClusteredGetResponseValidityFilter;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * The default distribution manager implementation
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class DistributionManagerImpl implements DistributionManager {
   private final Log log = LogFactory.getLog(DistributionManagerImpl.class);
   private final boolean trace = log.isTraceEnabled();
   Configuration configuration;
   ConsistentHash consistentHash;
   RpcManager rpcManager;
   CacheManagerNotifier notifier;
   int replCount;
   ViewChangeListener listener;
   CommandsFactory cf;

   @Inject
   public void init(Configuration configuration, RpcManager rpcManager, CacheManagerNotifier notifier, CommandsFactory cf) {
      this.configuration = configuration;
      this.rpcManager = rpcManager;
      this.notifier = notifier;
      this.cf = cf;
   }

   // needs to be AFTER the RpcManager
   @Start(priority = 20)
   public void start() throws Exception {
      replCount = configuration.getNumOwners();
      consistentHash = (ConsistentHash) Util.getInstance(configuration.getConsistentHashClass());
      consistentHash.setCaches(rpcManager.getTransport().getMembers());
      listener = new ViewChangeListener();
      notifier.addListener(listener);
   }

   @Stop(priority = 20)
   public void stop() {
      notifier.removeListener(listener);
   }

   public void rehash(Collection<Address> newList) {
      // on view change, we should update our view
      consistentHash.setCaches(newList);
   }

   public boolean isLocal(Object key) {
      return consistentHash.locate(key, replCount).contains(rpcManager.getTransport().getAddress());
   }

   public List<Address> locate(Object key) {
      return consistentHash.locate(key, replCount);
   }

   public Map<Object, List<Address>> locateAll(Collection<Object> keys) {
      return consistentHash.locateAll(keys, replCount);
   }

   public void transformForL1(CacheEntry entry) {
      if (entry.getLifespan() < 0 || entry.getLifespan() > configuration.getL1Lifespan())
         entry.setLifespan(configuration.getL1Lifespan());
   }

   public InternalCacheEntry retrieveFromRemoteSource(Object key) throws Exception {
      ClusteredGetCommand get = cf.buildClusteredGetCommand(key);

      ResponseFilter filter = new ClusteredGetResponseValidityFilter(locate(key));
      List<Response> responses = rpcManager.invokeRemotely(locate(key), get, ResponseMode.SYNCHRONOUS,
                                                           configuration.getSyncReplTimeout(), false, filter);

      if (!responses.isEmpty()) {
         for (Response r : responses) {
            if (r instanceof SuccessfulResponse) {
               return (InternalCacheEntry) ((SuccessfulResponse) r).getResponseValue();
            }
         }
      }

      return null;
   }

   @Listener
   public class ViewChangeListener {
      @ViewChanged
      public void handleViewChange(ViewChangedEvent e) {
         rehash(e.getNewMemberList());
      }
   }
}
