package org.infinispan.remoting.rpc;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.util.ByteString;
import org.testng.annotations.Test;

/**
 * @author anistor@redhat.com
 * @since 5.3
 */
@Test(testName = "remoting.rpc.RpcManagerCustomCacheRpcCommandTest", groups = "functional")
public class RpcManagerCustomCacheRpcCommandTest extends RpcManagerCustomReplicableCommandTest {

   @Override
   protected ReplicableCommand createReplicableCommandForTest(Object arg) {
      return new CustomCacheRpcCommand(ByteString.fromString(TEST_CACHE), arg);
   }
}
