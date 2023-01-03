package org.infinispan.api;

import org.infinispan.Cache;
import org.infinispan.commands.statetransfer.StateResponseCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.impl.EntryFactory;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.MagicKey;
import org.infinispan.remoting.inboundhandler.BlockHandler;
import org.infinispan.remoting.inboundhandler.ControllingPerCacheInboundInvocationHandler;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.CompletionStages;
import org.testng.annotations.Test;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.infinispan.test.TestingUtil.extractComponent;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertNull;

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

      final ControllingPerCacheInboundInvocationHandler handler = ControllingPerCacheInboundInvocationHandler.replace(futureBackupOwnerCache);
      final EntryFactory spyEntryFactory = spyEntryFactory(futureBackupOwnerCache);

      //it blocks the StateResponseCommand.class
      BlockHandler stateTransferHandler = handler.blockRpcBefore(StateResponseCommand.class);

      doAnswer(invocation -> {
         InvocationContext context = (InvocationContext) invocation.getArguments()[0];
         log.debugf("wrapEntryForWriting invoked with %s", context);

         CompletionStage<Void> stage = (CompletionStage<Void>) invocation.callRealMethod();
         CompletionStages.join(stage);
         assertNull(context.lookupEntry(key), "Entry should not be wrapped!");
         return stage;
      }).when(spyEntryFactory).wrapEntryForWriting(any(InvocationContext.class), any(), anyInt(),
            anyBoolean(), anyBoolean(), any());

      Future<?> killMemberResult = fork(() -> killMember(1));

      //await until the key is received from state transfer (the command is blocked now...)
      stateTransferHandler.awaitUntilBlocked();
      futureBackupOwnerCache.put(key, FINAL_VALUE);

      stateTransferHandler.unblock();
      killMemberResult.get(30, TimeUnit.SECONDS);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      builder.clustering()
            .hash().numOwners(2)
            .stateTransfer().fetchInMemoryState(true);
      createClusteredCaches(3, TestDataSCI.INSTANCE, builder);
   }

   private EntryFactory spyEntryFactory(Cache<Object, Object> cache) {
      EntryFactory spy = spy(extractComponent(cache, EntryFactory.class));
      TestingUtil.replaceComponent(cache, EntryFactory.class, spy, true);
      return spy;
   }
}
