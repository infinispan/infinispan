package org.infinispan.distribution;

import org.infinispan.config.Configuration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.logging.Log;
import org.infinispan.logging.LogFactory;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.Util;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * // TODO: Manik: Document this
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

   @Inject
   public void init(Configuration configuration, RpcManager rpcManager, CacheManagerNotifier notifier) {
      this.configuration = configuration;
      this.rpcManager = rpcManager;
      this.notifier = notifier;
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
      return consistentHash.locate(key, replCount).contains(rpcManager.getLocalAddress());
   }

   public List<Address> locate(Object key) {
      List<Address> adds = consistentHash.locate(key, replCount);
      if (trace) log.trace("Located {0} addresses for key {1}.  Repl count is {2}, addresses are {3}", adds.size(),
                           key, replCount, adds);
      return adds;
   }

   public Map<Object, List<Address>> locateAll(Collection<Object> keys) {
      return consistentHash.locateAll(keys, replCount);
   }

   @Listener
   public class ViewChangeListener {
      @ViewChanged
      public void handleViewChange(ViewChangedEvent e) {
         rehash(e.getNewMemberList());
      }
   }
}
