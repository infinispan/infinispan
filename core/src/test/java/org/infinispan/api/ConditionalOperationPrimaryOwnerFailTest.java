package org.infinispan.api;

import static org.infinispan.test.TestingUtil.extractComponent;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertNull;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.impl.EntryFactory;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.MagicKey;
import org.infinispan.reactive.publisher.impl.commands.batch.PublisherResponse;
import org.infinispan.remoting.responses.ValidResponse;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.impl.RequestRepository;
import org.infinispan.test.Mocks;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

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

      final EntryFactory spyEntryFactory = spyEntryFactory(futureBackupOwnerCache);
      final RequestRepository spyRequests = spyRequestRepository(futureBackupOwnerCache);

      //it blocks the StateResponseCommand.class
      final CountDownLatch latch1 = new CountDownLatch(1);
      final CountDownLatch latch2 = new CountDownLatch(1);
      doAnswer(invocation -> {
         log.debugf("Blocking response %s", invocation.getArguments()[2]);
         latch2.countDown();
         latch1.await(10, TimeUnit.SECONDS);
         return invocation.callRealMethod();
      }).when(spyRequests).addResponse(anyLong(), any(), argThat(r ->
         r.isSuccessful() && r instanceof ValidResponse<?> vr && vr.getResponseValue() instanceof PublisherResponse));

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
      latch2.await(30, TimeUnit.SECONDS);
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
      createClusteredCaches(3, TestDataSCI.INSTANCE, builder);
   }

   private EntryFactory spyEntryFactory(Cache<Object, Object> cache) {
      EntryFactory spy = spy(extractComponent(cache, EntryFactory.class));
      TestingUtil.replaceComponent(cache, EntryFactory.class, spy, true);
      return spy;
   }

   private RequestRepository spyRequestRepository(Cache<Object, Object> cache) {
      // This is assumed to be a JGroupsTransport
      Transport transport = extractComponent(cache, Transport.class);
      return Mocks.replaceFieldWithSpy(transport, "requests");
   }
}
