package org.infinispan.distribution;

import org.infinispan.Cache;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.config.Configuration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.remoting.ResponseMode;
import org.infinispan.remoting.RpcManager;

import java.util.List;

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

   public ConsistentHash getConsistentHash() {
      return consistentHash;
   }

   public boolean isLocal(Object key) {
      return consistentHash.locate(key, replCount).contains(rpcManager.getLocalAddress());
   }

   public Object retrieveFromRemote(Object key) throws Exception {
      // do a clustered get, unicast to the specific servers
      ClusteredGetCommand command = new ClusteredGetCommand(key, cache.getName());
      List<Object> responses = rpcManager.invokeRemotely(consistentHash.locate(key, replCount), command, ResponseMode.SYNCHRONOUS,
                                configuration.getSyncReplTimeout(), false);
      for (Object r: responses) {
         if (!(r instanceof Throwable))
            return r;
      }
      return null;
   }
}
