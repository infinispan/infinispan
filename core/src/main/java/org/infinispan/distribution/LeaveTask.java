package org.infinispan.distribution;

import org.infinispan.config.Configuration;
import org.infinispan.remoting.rpc.RpcManager;

/**
 * // TODO: Manik: Document this
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class LeaveTask extends RehashTask {
   protected LeaveTask(DistributionManagerImpl dmi, RpcManager rpcManager, Configuration configuration) {
      super(dmi, rpcManager, configuration);
   }

   protected void performRehash() throws Exception {
      // TODO: Customise this generated block
   }
}
