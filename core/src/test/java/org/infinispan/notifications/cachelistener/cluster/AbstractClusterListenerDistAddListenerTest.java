package org.infinispan.notifications.cachelistener.cluster;

import static org.infinispan.test.TestingUtil.getListeners;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.statetransfer.StateProvider;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.mockito.AdditionalAnswers;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

/**
 * Base class to be used for cluster listener tests for both tx and nontx distributed caches
 *
 * @author wburns
 * @since 7.0
 */
@Test(groups = "functional")
public abstract class AbstractClusterListenerDistAddListenerTest extends AbstractClusterListenerUtilTest {

   public static final String PRE_CLUSTER_LISTENERS_RELEASE = "pre_cluster_listeners_release_";
   public static final String POST_CLUSTER_LISTENERS_INVOKED = "post_cluster_listeners_invoked_";
   public static final String POST_CLUSTER_LISTENERS_RELEASE = "post_cluster_listeners_release_";
   public static final String PRE_CLUSTER_LISTENERS_INVOKED = "pre_cluster_listeners_invoked_";

   protected AbstractClusterListenerDistAddListenerTest(boolean tx) {
      super(tx, CacheMode.DIST_SYNC);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      builderUsed = new ConfigurationBuilder();
      // Disable conflict resolution on merge as StateProvider mocks are used in this#waitUntilRequestingListeners
      builderUsed.clustering().cacheMode(cacheMode).partitionHandling().mergePolicy(null);
      if (tx) {
         builderUsed.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
         builderUsed.locking().isolationLevel(IsolationLevel.READ_COMMITTED);
      }
      builderUsed.expiration().disableReaper();
      createClusteredCaches(3, CACHE_NAME, TestDataSCI.INSTANCE, builderUsed);
      injectTimeServices();
   }

   /**
    * This test is to verify then when a new node joins and a cluster listener is installed after the cluster listener
    * request is finished that it finds it.
    *
    * Node 1, 2, 3 exists
    * Node 2 adds cluster listener - knows to send to listener to Node 1
    * Node 4 starts up
    * Node 4 asks Node 1 for listeners (gets none)
    * Node 1 receives Node 2 listener
    *
    * Test needs to verify in this case that Node 3 gets the listener from Node 2
    */
   @Test
   public void testMemberJoinsWhileClusterListenerInstalled() throws TimeoutException, InterruptedException,
                                                                     ExecutionException {
      final Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      final Cache<Object, String> cache1 = cache(1, CACHE_NAME);

      CheckPoint checkPoint = new CheckPoint();
      waitUntilListenerInstalled(cache0, checkPoint);
      // We don't want this blocking
      checkPoint.triggerForever(POST_ADD_LISTENER_RELEASE + cache0);

      final ClusterListener clusterListener = new ClusterListener();
      Future<Void> future = fork(() -> {
         cache1.addListener(clusterListener);
         return null;
      });

      // Now wait until the listener is about to be installed on cache1
      checkPoint.awaitStrict(PRE_ADD_LISTENER_INVOKED + cache0, 10, TimeUnit.SECONDS);

      addClusteredCacheManager();

      // Now wait for cache3 to come up fully
      waitForClusterToForm(CACHE_NAME);

      final Cache<Object, String> cache3 = cache(3, CACHE_NAME);

      // Finally let the listener be added
      checkPoint.triggerForever(PRE_ADD_LISTENER_RELEASE + cache0);
      future.get(10, TimeUnit.SECONDS);

      MagicKey key = new MagicKey(cache3);
      verifySimpleInsertion(cache3, key, FIRST_VALUE, null, clusterListener, FIRST_VALUE);
   }

   /**
    * Ths test is very similar to {@link AbstractClusterListenerDistAddListenerTest#testMemberJoinsWhileClusterListenerInstalled} except that
    * the listener was retrieved in the initial request and thus would have received 2 callables to install the listener.
    * We need to make sure this doesn't cause 2 listeners to be installed causing duplicate messages.
    */
   @Test
   public void testMemberJoinsWhileClusterListenerInstalledDuplicate() throws TimeoutException, InterruptedException,
                                                                     ExecutionException {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      final Cache<Object, String> cache1 = cache(1, CACHE_NAME);

      CheckPoint checkPoint = new CheckPoint();
      waitUntilListenerInstalled(cache0, checkPoint);
      // We want the listener to be able to be added
      checkPoint.triggerForever(PRE_ADD_LISTENER_RELEASE + cache0);

      final ClusterListener clusterListener = new ClusterListener();
      Future<Void> future = fork(() -> {
         cache1.addListener(clusterListener);
         return null;
      });

      // Now wait until the listener is about to be installed on cache1
      checkPoint.awaitStrict(POST_ADD_LISTENER_INVOKED + cache0, 10, TimeUnit.SECONDS);

      addClusteredCacheManager();

      // Now wait for cache3 to come up fully
      waitForClusterToForm(CACHE_NAME);

      Cache<Object, String> cache3 = cache(3, CACHE_NAME);

      // Finally let the listener be added
      checkPoint.triggerForever(POST_ADD_LISTENER_RELEASE + cache0);
      future.get(10, TimeUnit.SECONDS);

      MagicKey key = new MagicKey(cache3);
      verifySimpleInsertion(cache3, key, FIRST_VALUE, null, clusterListener, FIRST_VALUE);
   }

   /**
    * This test is to make sure that if a new node comes up and requests the current cluster listeners that after
    * that is retrieved before processing the response that the node who has the cluster listener dies that we
    * don't keep the local cluster listener around for no reason
    * <p>
    * This may not be feasible since the cluster listener request is during topology change and
    */
   @Test
   public void testMemberJoinsAndRetrievesClusterListenersButMainListenerNodeDiesBeforeInstalled()
         throws TimeoutException, InterruptedException, ExecutionException {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      final Cache<Object, String> cache1 = cache(1, CACHE_NAME);

      final ClusterListener clusterListener = new ClusterListener();
      cache1.addListener(clusterListener);

      assertEquals(manager(0).getAddress(), manager(0).getMembers().get(0));

      CheckPoint checkPoint = new CheckPoint();
      waitUntilRequestingListeners(cache0, checkPoint);
      checkPoint.triggerForever(PRE_CLUSTER_LISTENERS_RELEASE + cache0);

      addClusteredCacheManager();

      Future<Cache<Object, String>> future = fork(() -> cache(3, CACHE_NAME));

      checkPoint.awaitStrict(POST_CLUSTER_LISTENERS_INVOKED + cache0, 10, TimeUnit.SECONDS);

      log.info("Killing node 1 ..");
      // Notice we are killing the manager that doesn't have a cache with the cluster listener
      TestingUtil.killCacheManagers(manager(1));
      cacheManagers.remove(1);
      log.info("Node 1 killed");

      checkPoint.triggerForever(POST_CLUSTER_LISTENERS_RELEASE + cache0);

      // Now wait for cache3 to come up fully
      TestingUtil.blockUntilViewsReceived(10000, false, cacheManagers);
      TestingUtil.waitForNoRebalance(caches(CACHE_NAME));

      Cache<Object, String> cache3 = future.get(10, TimeUnit.SECONDS);
      for (Object listener : getListeners(cache3)) {
         assertFalse(listener instanceof RemoteClusterListener);
      }
   }

   /**
    * Tests to make sure that if a new node is joining and the node it requested
    */
   @Test
   public void testNodeJoiningAndStateNodeDiesWithExistingClusterListener() throws TimeoutException,
                                                                                   InterruptedException,
                                                                                   ExecutionException {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);
      Cache<Object, String> cache2 = cache(2, CACHE_NAME);

      int initialCache0ListenerSize = getListeners(cache0).size();
      int initialCache1ListenerSize = getListeners(cache1).size();
      int initialCache2ListenerSize = getListeners(cache2).size();

      ClusterListener clusterListener = new ClusterListener();
      cache2.addListener(clusterListener);

      assertEquals(getListeners(cache0).size(), initialCache0ListenerSize +
            (cacheMode.isDistributed() ? 1 : 0));
      assertEquals(getListeners(cache1).size(), initialCache1ListenerSize +
            (cacheMode.isDistributed() ? 1 : 0));
      assertEquals(getListeners(cache2).size(), initialCache2ListenerSize + 1);

      assertEquals(manager(0).getAddress(), manager(0).getMembers().get(0));

      CheckPoint checkPoint = new CheckPoint();

      waitUntilRequestingListeners(cache0, checkPoint);
      checkPoint.triggerForever(POST_CLUSTER_LISTENERS_RELEASE + cache0);

      addClusteredCacheManager();

      Future<Cache<Object, String>> future = fork(() -> cache(3, CACHE_NAME));

      checkPoint.awaitStrict(PRE_CLUSTER_LISTENERS_INVOKED + cache0, 10, TimeUnit.SECONDS);

      log.info("Killing node 0 ..");
      // Notice we are killing the manager that doesn't have a cache with the cluster listener
      TestingUtil.killCacheManagers(manager(0));
      cacheManagers.remove(0);
      log.info("Node 0 killed");

      TestingUtil.blockUntilViewsReceived(10000, false, cacheManagers);
      TestingUtil.waitForNoRebalance(caches(CACHE_NAME));

      checkPoint.triggerForever(PRE_CLUSTER_LISTENERS_INVOKED + cache0);

      Cache<Object, String> cache3 = future.get(10, TimeUnit.SECONDS);

      MagicKey key = new MagicKey(cache3);

      verifySimpleInsertion(cache3, key, FIRST_VALUE, null, clusterListener, FIRST_VALUE);
   }

   /**
    * The premise is the same as {@link AbstractClusterListenerDistAddListenerTest#testNodeJoiningAndStateNodeDiesWithExistingClusterListener}.
    * This also has the twist of the fact that the node who dies is also has the cluster listener.  This test makes sure
    * that the subsequent node asked for cluster listeners hasn't yet got the view change and still has the cluster
    * listener in it.  Also the requesting node should have the view change before installing.
    */
   @Test(enabled = false, description = "Test may not be doable, check TODO in test")
   public void testNodeJoiningAndStateNodeDiesWhichHasClusterListener() throws TimeoutException,
                                                                               InterruptedException,
                                                                               ExecutionException {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);
      Cache<Object, String> cache2 = cache(2, CACHE_NAME);

      int initialCache0ListenerSize = getListeners(cache0).size();
      int initialCache1ListenerSize = getListeners(cache1).size();
      int initialCache2ListenerSize = getListeners(cache2).size();

      ClusterListener clusterListener = new ClusterListener();
      cache0.addListener(clusterListener);

      assertEquals(getListeners(cache0).size(), initialCache0ListenerSize + 1);
      assertEquals(getListeners(cache1).size(), initialCache1ListenerSize + 1);
      assertEquals(getListeners(cache2).size(), initialCache2ListenerSize + 1);

      // Make sure cache0 will  be the one will get the cluster listener request
      assertEquals(manager(0).getAddress(), manager(0).getMembers().get(0));

      CheckPoint checkPoint = new CheckPoint();

      waitUntilRequestingListeners(cache0, checkPoint);
      checkPoint.triggerForever(POST_CLUSTER_LISTENERS_RELEASE + cache0);
      waitUntilViewChangeOccurs(manager(1), "manager1", checkPoint);

      // We let the first view change occur just fine on cache1 (this will be the addition of cache3).
      // What we want to block is the second one which is the removal of cache0
      checkPoint.trigger(PRE_VIEW_LISTENER_RELEASE + "manager1");

      addClusteredCacheManager();

      waitUntilViewChangeOccurs(manager(3), "manager3", checkPoint);
      // We don't want to block the view listener change on cache3
      checkPoint.trigger(PRE_VIEW_LISTENER_RELEASE + "manager3");

      Future<Cache<Object, String>> future = fork(() -> cache(3, CACHE_NAME));

      // Wait for view change to occur on cache1 for the addition of cache3
      // Note we haven't triggered the view change for cache1 for the following removal yet
      checkPoint.awaitStrict(POST_VIEW_LISTENER_INVOKED + "manager1", 10, TimeUnit.SECONDS);
      // Wait for the cluster listener request to come into cache0 which is from cache3
      checkPoint.awaitStrict(PRE_CLUSTER_LISTENERS_INVOKED + cache0, 10, TimeUnit.SECONDS);

      // Now we kill cache0 while it is processing the request from cache3, which will in turn force it to ask cache1
      log.info("Killing node 0 ..");
      TestingUtil.killCacheManagers(manager(0));
      cacheManagers.remove(0);
      log.info("Node 0 killed");

      // TODO: need a away to verify that the response was sent back to cache3 before releasing the next line.  However
      // with no reference to cache I don't think this is possible.  If this can be fixed then test can be reenabled
      Cache<Object, String> cache3 = future.get(10, TimeUnit.SECONDS);

      // Now we can finally let the view change complete on cache1
      checkPoint.triggerForever(PRE_VIEW_LISTENER_RELEASE + "manager1");

      // Now wait for cache3 to come up fully
      TestingUtil.blockUntilViewsReceived(60000, false, cache1, cache2);
      TestingUtil.waitForNoRebalance(cache1, cache2);

      MagicKey key = new MagicKey(cache3);
      cache3.put(key, FIRST_VALUE);

      assertEquals(getListeners(cache1).size(), initialCache1ListenerSize);
      assertEquals(getListeners(cache2).size(), initialCache2ListenerSize);

      // Since we can't get a reliable start count, make sure no RemoteClusterListener is present which is added for
      // a cluster listener
      for (Object listener : getListeners(cache3)) {
         assertFalse(listener instanceof RemoteClusterListener);
      }
   }

   protected void waitUntilRequestingListeners(final Cache<?, ?> cache, final CheckPoint checkPoint) {
      StateProvider sp = TestingUtil.extractComponent(cache, StateProvider.class);
      final Answer<Object> forwardedAnswer = AdditionalAnswers.delegatesTo(sp);
      StateProvider mockProvider = mock(StateProvider.class, withSettings().defaultAnswer(forwardedAnswer));
      doAnswer(invocation -> {
         // Wait for main thread to sync up
         checkPoint.trigger(PRE_CLUSTER_LISTENERS_INVOKED + cache);
         // Now wait until main thread lets us through
         checkPoint.awaitStrict(PRE_CLUSTER_LISTENERS_RELEASE + cache, 10, TimeUnit.SECONDS);

         try {
            return forwardedAnswer.answer(invocation);
         } finally {
            // Wait for main thread to sync up
            checkPoint.trigger(POST_CLUSTER_LISTENERS_INVOKED + cache);
            // Now wait until main thread lets us through
            checkPoint.awaitStrict(POST_CLUSTER_LISTENERS_RELEASE + cache, 10, TimeUnit.SECONDS);
         }
      }).when(mockProvider).getClusterListenersToInstall();
      TestingUtil.replaceComponent(cache, StateProvider.class, mockProvider, true);
   }
}
