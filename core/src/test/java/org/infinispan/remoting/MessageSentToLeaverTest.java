package org.infinispan.remoting;

import java.util.Collection;
import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Test that CommandAwareRpcManager detects members who left the cluster and throws an exception.
 *
 * @author Dan Berindei <dan@infinispan.org>
 */
@Test (testName = "remoting.MessageSentToLeaverTest", groups = "functional")
public class MessageSentToLeaverTest extends AbstractInfinispanTest {

   public void testGroupRequestSentToMemberAfterLeaving() {
      EmbeddedCacheManager cm1 = null, cm2 = null, cm3 = null;
      try {
         ConfigurationBuilder c = new ConfigurationBuilder();
         c
            .clustering().cacheMode(CacheMode.REPL_SYNC)
               .hash().numOwners(3);

         cm1 = TestCacheManagerFactory.createClusteredCacheManager(c);
         cm2 = TestCacheManagerFactory.createClusteredCacheManager(c);
         cm3 = TestCacheManagerFactory.createClusteredCacheManager(c);

         Cache<Object,Object> c1 = cm1.getCache();
         Cache<Object, Object> c2 = cm2.getCache();
         Cache<Object, Object> c3 = cm3.getCache();

         TestingUtil.blockUntilViewsReceived(30000, c1, c2, c3);

         c2.put("k", "v1");

         RpcManager rpcManager = TestingUtil.extractComponent(c1, RpcManager.class);
         Collection<Address>  addresses = cm1.getMembers();

         CommandsFactory cf = TestingUtil.extractCommandsFactory(c1);
         PutKeyValueCommand cmd = cf.buildPutKeyValueCommand("k", "v2",
               new EmbeddedMetadata.Builder().build(), EnumUtil.EMPTY_BIT_SET);

         Map<Address,Response> responseMap = rpcManager.invokeRemotely(addresses, cmd, rpcManager.getDefaultRpcOptions(true, DeliverOrder.NONE));
         assert responseMap.size() == 2;

         TestingUtil.killCacheManagers(cm2);
         TestingUtil.blockUntilViewsReceived(30000, false, c1, c3);

         try {
            rpcManager.invokeRemotely(addresses, cmd, rpcManager.getDefaultRpcOptions(true, DeliverOrder.NONE));
            assert false: "invokeRemotely should have thrown an exception";
         } catch (SuspectException e) {
            // expected
         }
      } finally {
         TestingUtil.killCacheManagers(cm1, cm2, cm3);
      }
   }

}
