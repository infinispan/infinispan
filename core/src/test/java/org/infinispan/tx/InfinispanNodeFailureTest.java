package org.infinispan.tx;

import static org.infinispan.test.TestingUtil.waitForNoRebalance;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.MagicKey;
import org.infinispan.interceptors.BaseCustomAsyncInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.transport.DelayedViewJGroupsTransport;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

/**
 * This test reproduces following scenario in Infinispan:
 * <pre>
 * NODE-A                        NODE-B                      NODE-C
 *
 * 1:start-tx
 * 1:replace key X
 * 1:lock X (A is owner)
 * 1:put key Z
 *                               1:lock Z (B is owner)
 *                                                           kill node C
 * 1:get response from lock Z
 * 1:release ALL locks (!)
 * new view is received
 * 1:retry lock Z
 *                               1:lock Z (B is owner)
 *                               2:start-tx
 *                               2:replace key X
 * 2:lock X (A is owner)
 *                               2:commit-tx
 * 1:commit-tx
 *
 * The problematic part is marked with exclamation, PessimisticLockingInterceptor releases ALL locks and retries just
 * one last command, which puts
 * current transaction in invalid state, when client thinks first operation protects second operation with a lock, but
 * this is not the case.
 * </pre>
 *
 * @since 9.0
 */
@Test(groups = "functional", testName = "tx.InfinispanNodeFailureTest")
public class InfinispanNodeFailureTest extends MultipleCacheManagersTest {

   private static final Integer INITIAL_VALUE = 0;
   private static final Integer REPLACING_VALUE = 1;
   private static final String TEST_CACHE = "test_cache";

   private CountDownLatch viewLatch;

   public void killedNodeDoesNotBreakReplaceCommand() throws Exception {
      defineConfigurationOnAllManagers(TEST_CACHE, new ConfigurationBuilder().read(manager(0).getDefaultCacheConfiguration()));
      waitForClusterToForm(TEST_CACHE);
      waitForNoRebalance(caches(TEST_CACHE));

      final Object replaceKey = new MagicKey("X", cache(0, TEST_CACHE));
      final Object putKey = new MagicKey("Z", cache(1, TEST_CACHE));

      cache(0, TEST_CACHE).put(replaceKey, INITIAL_VALUE);

      // prepare third node to notify us when put command is in progress so we can kill the node
      final CountDownLatch beforeKill = new CountDownLatch(1);
      final CountDownLatch afterKill = new CountDownLatch(1);

      advancedCache(1, TEST_CACHE).getAsyncInterceptorChain().addInterceptor(new BaseCustomAsyncInterceptor() {

         @Override
         public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) throws Throwable {
            return invokeNextAndFinally(ctx, command, (rCtx, rCommand, rv, t) -> {
               LockControlCommand cmd = (LockControlCommand) rCommand;
               if (putKey.equals(cmd.getSingleKey())) {
                  // notify main thread it can start killing third node
                  beforeKill.countDown();
                  // wait for completion and proceed
                  afterKill.await(10, TimeUnit.SECONDS);
               }
            });
         }
      }, 1);

      // execute replace command in separate thread so we can do something else meanwhile
      Future<Boolean> firstResult = fork(() -> {
         try {
            tm(0, TEST_CACHE).begin();

            // this should replace and lock REPLACE_KEY so other transactions can't pass this barrier
            boolean result = cache(0, TEST_CACHE).replace(replaceKey, INITIAL_VALUE, REPLACING_VALUE);

            // issue put command so it is retried while node-c is being killed
            cache(0, TEST_CACHE).put(putKey, "some-value");

            // apply new view
            viewLatch.countDown();

            tm(0, TEST_CACHE).commit();
            return result;
         } catch (Throwable t) {
            return null;
         }
      });

      // wait third node to complete replace command and kill it
      assertTrue(beforeKill.await(10, TimeUnit.SECONDS));
      // kill node-c, do not wait rehash, it is important to continue with put-retry before new view is received
      killMember(2, TEST_CACHE, false);
      afterKill.countDown();

      tm(1, TEST_CACHE).begin();
      // this replace should never succeed because first node has already replaced and locked value
      // but during put command replace lock is lost, so we can successfully replace the same value again, which is a bug
      boolean secondResult = cache(1, TEST_CACHE).replace(replaceKey, INITIAL_VALUE, REPLACING_VALUE);
      tm(1, TEST_CACHE).commit();

      // check that first node did not fail
      assertEquals(Boolean.TRUE, firstResult.get());
      assertEquals(REPLACING_VALUE, cache(0, TEST_CACHE).get(replaceKey));
      assertEquals(REPLACING_VALUE, cache(1, TEST_CACHE).get(replaceKey));

      // check that second node state is inconsistent, second result should be FALSE in read committed pessimistic cache
      // uncomment when this bug is fixed
      assertEquals(false, secondResult);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder configuration = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      configuration.locking()
            .useLockStriping(false)
            .isolationLevel(IsolationLevel.READ_COMMITTED)
            .lockAcquisitionTimeout(20000);
      configuration.transaction()
            .transactionManagerLookup(new EmbeddedTransactionManagerLookup())
            .lockingMode(LockingMode.PESSIMISTIC)
            .useSynchronization(false)
            .recovery()
            .disable();
      configuration.clustering()
            .hash()
            .numSegments(60)
            .stateTransfer()
            .fetchInMemoryState(false);

      viewLatch = new CountDownLatch(1);
      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
      global.transport().transport(new DelayedViewJGroupsTransport(viewLatch));
      addClusterEnabledCacheManager(global, configuration);
      addClusterEnabledCacheManager(configuration);
      addClusterEnabledCacheManager(configuration);
   }

}
