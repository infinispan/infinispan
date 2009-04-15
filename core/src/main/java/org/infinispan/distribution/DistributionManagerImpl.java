package org.infinispan.distribution;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.remoting.RpcManager;
import org.infinispan.remoting.transport.Address;

import java.util.List;
import java.util.Map;

/**
 * // TODO: Manik: Document this
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class DistributionManagerImpl implements DistributionManager {

   Configuration configuration;
   ConsistentHash consistentHash;
   RpcManager rpcManager;
   int replCount;
   Cache cache;

   @Inject
   public void init(Cache cache, Configuration configuration, RpcManager rpcManager) {
      this.cache = cache;
      this.configuration = configuration;
      this.rpcManager = rpcManager;
   }

   @Start
   public void start() {
      replCount = 2; // TODO read this from the configuration
      consistentHash = new DefaultConsistentHash(); // TODO read this from the configuration
   }

   public void rehash() {
      // TODO: Customise this generated block
   }

   public boolean isLocal(Object key) {
      return consistentHash.locate(key, replCount).contains(rpcManager.getLocalAddress());
   }

   public List<Address> locate(Object key) {
      return null;  // TODO: Customise this generated block
   }

   public Map<Object, List<Address>> locate(List<Object> keys) {
      return null;  // TODO: Customise this generated block
   }
}
