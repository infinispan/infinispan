package org.infinispan.api;

import org.infinispan.Cache;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.EntryFactory;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.MagicKey;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.Reply;
import org.infinispan.statetransfer.StateResponseCommand;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.replaceField;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.testng.AssertJUnit.assertFalse;

/**
 * Tests if the keys are not wrapped in the non-owner nodes
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "api.ConditionalOperationPrimaryOwnerFailTest")
public class ConditionalOperationPrimaryOwnerFailTest extends MultipleCacheManagersTest {

   private static final String INITIAL_VALUE = "initial";
   private static final String FINAL_VALUE = "final";

   public void testEntryNotWrapped() throws Throwable {
      assertClusterSize("Wrong cluster size!", 3);
      final Object key = new MagicKey(cache(0), cache(1));
      final Cache<Object, Object> futureBackupOwnerCache = cache(2);

      cache(0).put(key, INITIAL_VALUE);

      final PerCacheInboundInvocationHandler spyHandler = spyInvocationHandler(futureBackupOwnerCache);
      final EntryFactory spyEntryFactory = spyEntryFactory(futureBackupOwnerCache);

      //it blocks the StateResponseCommand.class
      final CountDownLatch latch1 = new CountDownLatch(1);
      final CountDownLatch latch2 = new CountDownLatch(1);
      doAnswer(new Answer<Object>() {
         @Override
         public Object answer(InvocationOnMock invocation) throws Throwable {
            CacheRpcCommand command = (CacheRpcCommand) invocation.getArguments()[0];
            if (command instanceof StateResponseCommand) {
               log.debugf("Blocking command %s", command);
               latch2.countDown();
               latch1.await();
            }
            return invocation.callRealMethod();
         }
      }).when(spyHandler).handle(any(CacheRpcCommand.class), any(Reply.class), any(DeliverOrder.class));

      doAnswer(new Answer() {
         @Override
         public Object answer(InvocationOnMock invocation) throws Throwable {
            InvocationContext context = (InvocationContext) invocation.getArguments()[0];
            log.debugf("wrapEntryForPut invoked with %s", context);
            assertFalse("Entry should not be wrapped!", context.isOriginLocal());
            return invocation.callRealMethod();
         }
      }).when(spyEntryFactory).wrapEntryForPut(any(InvocationContext.class), anyObject(),
                                               any(InternalCacheEntry.class), anyBoolean(),
                                               any(FlagAffectedCommand.class), anyBoolean());

      Future<?> killMemberResult = fork(new Runnable() {
         @Override
         public void run() {
            killMember(1);
         }
      });

      //await until the key is received from state transfer (the command is blocked now...)
      latch2.await();
      futureBackupOwnerCache.put(key, FINAL_VALUE);

      latch1.countDown();
      killMemberResult.get(30, TimeUnit.SECONDS);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      builder.clustering()
            .hash().numOwners(2)
            .stateTransfer().fetchInMemoryState(true);
      createClusteredCaches(3, builder);
   }

   private EntryFactory spyEntryFactory(Cache<Object, Object> cache) {
      EntryFactory spy = spy(extractComponent(cache, EntryFactory.class));
      TestingUtil.replaceComponent(cache, EntryFactory.class, spy, true);
      return spy;
   }

   private PerCacheInboundInvocationHandler spyInvocationHandler(Cache cache) {
      PerCacheInboundInvocationHandler spy = Mockito.spy(extractComponent(cache, PerCacheInboundInvocationHandler.class));
      TestingUtil.replaceComponent(cache, PerCacheInboundInvocationHandler.class, spy, true);
      replaceField(spy, "inboundInvocationHandler", cache.getAdvancedCache().getComponentRegistry(), ComponentRegistry.class);
      return spy;
   }

}
