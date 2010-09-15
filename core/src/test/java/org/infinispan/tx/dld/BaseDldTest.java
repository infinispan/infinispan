package org.infinispan.tx.dld;

import org.infinispan.Cache;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
public abstract class BaseDldTest extends MultipleCacheManagersTest {

   protected ControlledRpcManager rpcManager0;
   protected ControlledRpcManager rpcManager1;   

   public static ControlledRpcManager replaceRpcManager(Cache cache) {
      RpcManager rpcManager1 = TestingUtil.extractComponent(cache, RpcManager.class);
      ControlledRpcManager controlledRpcManager1 = new ControlledRpcManager(rpcManager1);
      TestingUtil.replaceComponent(cache, RpcManager.class, controlledRpcManager1, true);
      return controlledRpcManager1;
   }

}
