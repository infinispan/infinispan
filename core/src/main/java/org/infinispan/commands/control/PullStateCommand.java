package org.infinispan.commands.control;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.config.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.ConsistentHash;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
import org.infinispan.marshall.Ids;
import org.infinispan.marshall.Marshallable;
import org.infinispan.marshall.exts.ReplicableCommandExternalizer;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * // TODO: Manik: Document this
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Marshallable(externalizer = ReplicableCommandExternalizer.class, id = Ids.PULL_STATE_COMMAND)
public class PullStateCommand extends BaseRpcCommand {

   public static final int COMMAND_ID = 19;
   Address requestor;
   DataContainer dataContainer;
   CacheLoaderManager clm;
   Transport transport;
   ConsistentHash newCH, oldCH;
   Address self;
   DistributionManager distributionManager;
   Configuration configuration;
   int numCopies;

   public PullStateCommand() {
   }

   public PullStateCommand(String cacheName, Address requestor, ConsistentHash newCH) {
      super(cacheName);
      this.requestor = requestor;
      this.newCH = newCH;
   }

   public PullStateCommand(Transport transport) {
      this.transport = transport;
   }

   public void init(DataContainer dataContainer, CacheLoaderManager clm, DistributionManager distributionManager, Configuration c) {
      this.dataContainer = dataContainer;
      this.clm = clm;
      this.distributionManager = distributionManager;
      this.configuration = c;
   }

   /**
    * @param ctx invocation context
    * @return Should return a Map<Object, InternalCacheValue>
    * @throws Throwable
    */
   public Object perform(InvocationContext ctx) throws Throwable {
      self = transport.getAddress();
      oldCH = distributionManager.getConsistentHash();
      numCopies = configuration.getNumOwners();

      Map<Object, InternalCacheValue> state = new HashMap<Object, InternalCacheValue>();
      for (InternalCacheEntry ice : dataContainer) {
         Object k = ice.getKey();
         if (shouldAddToMap(k)) {
            state.put(k, ice.toInternalCacheValue());
         }
      }

      CacheStore cacheStore = getCacheStore();
      if (cacheStore != null) {
         for (InternalCacheEntry ice : cacheStore.loadAll()) {
            Object k = ice.getKey();
            if (shouldAddToMap(k) && !state.containsKey(k)) {
               state.put(k, ice.toInternalCacheValue());
            }
         }
      }
      return state;
   }

   final boolean shouldAddToMap(Object k) {
      // if the current address is the current "owner" of this key (in old_ch), and the requestor is in the owner list
      // in new_ch, then add this to the map.
      List<Address> oldOwnerList = oldCH.locate(k, numCopies);
      if (oldOwnerList.size() > 0 && self.equals(oldOwnerList.get(0))) {
         List<Address> newOwnerList = newCH.locate(k, numCopies);
         if (newOwnerList.contains(requestor)) return true;
      }
      return false;
   }

   final CacheStore getCacheStore() {
      return clm != null && clm.isEnabled() ? clm.getCacheStore() : null;
   }

   public byte getCommandId() {
      return COMMAND_ID;
   }

   public Object[] getParameters() {
      return new Object[]{cacheName, requestor, newCH};
   }

   public void setParameters(int commandId, Object[] parameters) {
      cacheName = (String) parameters[0];
      requestor = (Address) parameters[1];
      newCH = (ConsistentHash) parameters[2];
   }
}
