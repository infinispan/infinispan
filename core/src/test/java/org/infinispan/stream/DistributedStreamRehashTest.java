package org.infinispan.stream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.testng.AssertJUnit.assertEquals;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.stream.impl.LocalStreamManager;
import org.infinispan.stream.impl.TerminalOperation;
import org.infinispan.stream.impl.termop.SegmentRetryingOperation;
import org.infinispan.test.Mocks;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.test.fwk.InCacheMode;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.mockito.AdditionalAnswers;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

/**
 * Some tests to verify that
 * @author wburns
 * @since 10.0
 */
@Test(groups = "functional", testName = "streams.DistributedStreamRehashTest")
@InCacheMode({ CacheMode.DIST_SYNC })
public class DistributedStreamRehashTest extends MultipleCacheManagersTest {
   protected final String CACHE_NAME = "rehashStreamCache";

   private ControlledConsistentHashFactory consistentHashFactory;

   @Override
   protected void createCacheManagers() throws Throwable {
      consistentHashFactory = new ControlledConsistentHashFactory.Default(new int[][]{{0, 1}, {1, 2},
            {2, 3}, {3, 0}});
      ConfigurationBuilder builderUsed = new ConfigurationBuilder();
      builderUsed.clustering().cacheMode(cacheMode);
      if (cacheMode == CacheMode.DIST_SYNC) {
         builderUsed.clustering().clustering().hash().numOwners(2).numSegments(4).consistentHashFactory(consistentHashFactory);
      }
      createClusteredCaches(4, CACHE_NAME, builderUsed);
   }

   public void testNodeFailureDuringProcessingForCollect() throws InterruptedException, TimeoutException, ExecutionException {
      // Insert a key into each cache - so we always have values to find on each node
      for (Cache<MagicKey, Object> cache : this.<MagicKey, Object>caches(CACHE_NAME)) {
         MagicKey key = new MagicKey(cache);
         cache.put(key, key.toString());
      }

      Cache<MagicKey, Object> originator = cache(0, CACHE_NAME);
      // We stop the #1 node which equates to entries store in segment 2
      Cache<MagicKey, Object> nodeToBlockBeforeProcessing = cache(1, CACHE_NAME);
      Cache<MagicKey, Object> nodeToStop = cache(3, CACHE_NAME);

      LocalStreamManager<Map.Entry<MagicKey, Object>, MagicKey> localStreamManager = TestingUtil.extractComponent(
            nodeToBlockBeforeProcessing, LocalStreamManager.class);
      LocalStreamManager<Map.Entry<MagicKey, Object>, MagicKey> spiedManager = spy(localStreamManager);

      CheckPoint checkPoint = new CheckPoint();
      // Let the supplier supply the stream, but don't let it process it yet
      checkPoint.triggerForever(Mocks.BEFORE_RELEASE);

      doAnswer(invocation -> {
         Object[] arguments = invocation.getArguments();
         int offset = 8;
         TerminalOperation<Map.Entry<MagicKey, Object>, MagicKey> terminalOperation = (TerminalOperation) arguments[offset];

         Answer opAnswer = AdditionalAnswers.delegatesTo(terminalOperation);

         TerminalOperation<Map.Entry<MagicKey, Object>, MagicKey> mockOperation = Mockito.mock(TerminalOperation.class, opAnswer);

         // Make sure to provide a supplier that we can control the blocking
         doAnswer(opInvocation -> {
            Answer supplierAnswer = AdditionalAnswers.delegatesTo(opInvocation.getArgument(0));

            Supplier<Stream<Map.Entry<MagicKey, Object>>> mockSupplier = Mockito.mock(Supplier.class, supplierAnswer);

            Answer blockingAnswer = Mocks.blockingAnswer(supplierAnswer, checkPoint);
            doAnswer(blockingAnswer).when(mockSupplier).get();

            return opInvocation.getMethod().invoke(terminalOperation, mockSupplier);
         }).when(mockOperation).setSupplier(any());

         // Replace the argument so they get our spy who will block
         arguments[offset] = mockOperation;


         return Mocks.invokeAndReturnMock(invocation, localStreamManager);
      }).when(spiedManager).streamOperationRehashAware(any(), any(), anyBoolean(), any(), any(), any(), anyBoolean(),
            anyBoolean(), isA(SegmentRetryingOperation.class));

      TestingUtil.replaceComponent(nodeToBlockBeforeProcessing, LocalStreamManager.class, spiedManager, true);

      Future<List<Map.Entry<MagicKey, Object>>> future = fork(() ->
            originator.entrySet().stream().collect(() -> Collectors.toList()));

      // Make sure we are up to sync
      checkPoint.awaitStrict(Mocks.AFTER_INVOCATION, 10, TimeUnit.SECONDS);

      // Note that segment 2 doesn't map to the node1 anymore
      consistentHashFactory.setOwnerIndexes(new int[][]{{0, 1}, {0, 2}, {2, 1}, {1, 0}});

      // Have to remove the cache manager so the cluster formation can work properly
      cacheManagers.remove(cacheManagers.size() - 1);
      nodeToStop.getCacheManager().stop();

      TestingUtil.blockUntilViewsReceived((int) TimeUnit.SECONDS.toMillis(10), false, caches(CACHE_NAME));

      TestingUtil.waitForNoRebalance(caches(CACHE_NAME));

      // Now let the stream be processed
      checkPoint.triggerForever(Mocks.AFTER_RELEASE);

      List<Map.Entry<MagicKey, Object>> list = future.get(10, TimeUnit.SECONDS);

      assertEquals(cacheManagers.size() + 1, list.size());
   }
}
