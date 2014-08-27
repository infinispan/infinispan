package org.infinispan.notifications.cachelistener.cluster;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.distribution.MagicKey;
import org.infinispan.statetransfer.StateProvider;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.mockito.AdditionalAnswers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

/**
 * Base class to be used for cluster listener tests for both tx and nontx distributed caches
 *
 * @author wburns
 * @since 7.0
 */
@Test(groups = "functional")
public abstract class AbstractClusterListenerDistAddListenerTest extends AbstractClusterListenerUtilTest {
   protected AbstractClusterListenerDistAddListenerTest(boolean tx) {
      super(tx, CacheMode.DIST_SYNC);
   }

   /**
    * This test is to verify then when a new node joins and a cluster listener is installed after the cluster listener
    * request is finished that it finds it.
    *
    * Node 1, 2 & 3 exists
    * Node 2 adds cluster listener - knows to send to listener to Node 1
    * Node 4 starts up
    * Node 4 asks Node 1 for listeners (gets none)
    * Node 1 receives Node 2 listener
    *
    * Test needs to verify in this case that Nod3 3 gets the listener from Node 2
    */
   @Test
   public void testMemberJoinsWhileClusterListenerInstalled() throws TimeoutException, InterruptedException,
                                                                     ExecutionException {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      final Cache<Object, String> cache1 = cache(1, CACHE_NAME);

      CheckPoint checkPoint = new CheckPoint();
      waitUntilListenerInstalled(cache0, checkPoint);
      // We don't want this blocking
      checkPoint.triggerForever("post_add_listener_release_" + cache0);

      final ClusterListener clusterListener = new ClusterListener();
      Future<Void> future = fork(new Callable<Void>() {

         @Override
         public Void call() throws Exception {

            cache1.addListener(clusterListener);
            return null;
         }
      });

      // Now wait until the listener is about to be installed on cache1
      checkPoint.awaitStrict("pre_add_listener_invoked_" + cache0, 10, TimeUnit.SECONDS);

      // First we add the new node, but block the dist exec execution
      log.info("Adding a new node ..");
      addClusterEnabledCacheManager(builderUsed);
      log.info("Added a new node");

      // Now wait for cache3 to come up fully
      waitForClusterToForm(CACHE_NAME);

      Cache<Object, String> cache3 = cache(3, CACHE_NAME);

      // Finally let the listener be added
      checkPoint.triggerForever("pre_add_listener_release_" + cache0);
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
      checkPoint.triggerForever("pre_add_listener_release_" + cache0);

      final ClusterListener clusterListener = new ClusterListener();
      Future<Void> future = fork(new Callable<Void>() {

         @Override
         public Void call() throws Exception {

            cache1.addListener(clusterListener);
            return null;
         }
      });

      // Now wait until the listener is about to be installed on cache1
      checkPoint.awaitStrict("post_add_listener_invoked_" + cache0, 10, TimeUnit.SECONDS);

      // First we add the new node, but block the dist exec execution
      log.info("Adding a new node ..");
      addClusterEnabledCacheManager(builderUsed);
      log.info("Added a new node");

      // Now wait for cache3 to come up fully
      waitForClusterToForm(CACHE_NAME);

      Cache<Object, String> cache3 = cache(3, CACHE_NAME);

      // Finally let the listener be added
      checkPoint.triggerForever("post_add_listener_release_" + cache0);
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
      checkPoint.triggerForever("pre_cluster_listeners_release_" + cache0);

      // First we add the new node, but block the dist exec execution
      log.info("Adding a new node ..");
      addClusterEnabledCacheManager(builderUsed);
      log.info("Added a new node");

      Future<Cache<Object, String>> future = fork(new Callable<Cache<Object, String>>() {
         @Override
         public Cache<Object, String> call() throws Exception {
            return cache(3, CACHE_NAME);
         }
      });

      checkPoint.awaitStrict("post_cluster_listeners_invoked_" + cache0, 10, TimeUnit.SECONDS);

      log.info("Killing node 1 ..");
      // Notice we are killing the manager that doesn't have a cache with the cluster listener
      TestingUtil.killCacheManagers(manager(1));
      cacheManagers.remove(1);
      log.info("Node 1 killed");

      checkPoint.triggerForever("post_cluster_listeners_release_" + cache0);

      // Now wait for cache3 to come up fully
      TestingUtil.blockUntilViewsReceived(10000, false, cacheManagers);
      TestingUtil.waitForRehashToComplete(caches(CACHE_NAME));

      Cache<Object, String> cache3 = future.get(10, TimeUnit.SECONDS);

      for (Object listener : cache3.getAdvancedCache().getListeners()) {
         assertFalse(listener instanceof RemoteClusterListener);
      }
   }

   /**
    * Tests to make sure that if a new node is joining and the node it requested
    * @throws TimeoutException
    * @throws InterruptedException
    * @throws ExecutionException
    */
   @Test
   public void testNodeJoiningAndStateNodeDiesWithExistingClusterListener() throws TimeoutException,
                                                                                   InterruptedException,
                                                                                   ExecutionException {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);
      Cache<Object, String> cache2 = cache(2, CACHE_NAME);

      int initialCache0ListenerSize = cache0.getAdvancedCache().getListeners().size();
      int initialCache1ListenerSize = cache1.getAdvancedCache().getListeners().size();
      int initialCache2ListenerSize = cache2.getAdvancedCache().getListeners().size();

      ClusterListener clusterListener = new ClusterListener();
      cache2.addListener(clusterListener);

      assertEquals(cache0.getAdvancedCache().getListeners().size(), initialCache0ListenerSize +
            (cacheMode.isDistributed() ? 1 : 0));
      assertEquals(cache1.getAdvancedCache().getListeners().size(), initialCache1ListenerSize +
            (cacheMode.isDistributed() ? 1 : 0));
      assertEquals(cache2.getAdvancedCache().getListeners().size(), initialCache2ListenerSize + 1);

      assertEquals(manager(0).getAddress(), manager(0).getMembers().get(0));

      CheckPoint checkPoint = new CheckPoint();

      waitUntilRequestingListeners(cache0, checkPoint);
      checkPoint.triggerForever("post_cluster_listeners_release_" + cache0);

      // First we add the new node, but block the dist exec execution
      log.info("Adding a new node ..");
      addClusterEnabledCacheManager(builderUsed);
      log.info("Added a new node");

      Future<Cache<Object, String>> future = fork(new Callable<Cache<Object, String>>() {
         @Override
         public Cache<Object, String> call() throws Exception {
            return cache(3, CACHE_NAME);
         }
      });

      checkPoint.awaitStrict("pre_cluster_listeners_invoked_" + cache0, 10, TimeUnit.SECONDS);

      log.info("Killing node 0 ..");
      // Notice we are killing the manager that doesn't have a cache with the cluster listener
      TestingUtil.killCacheManagers(manager(0));
      cacheManagers.remove(0);
      log.info("Node 0 killed");

      TestingUtil.blockUntilViewsReceived(10000, false, cacheManagers);
      TestingUtil.waitForRehashToComplete(caches(CACHE_NAME));

      checkPoint.triggerForever("pre_cluster_listeners_invoked_" + cache0);

      Cache<Object, String> cache3 = future.get(10, TimeUnit.SECONDS);

      MagicKey key = new MagicKey(cache3);

      verifySimpleInsertion(cache3, key, FIRST_VALUE, null, clusterListener, FIRST_VALUE);
   }

   /**
    * The premise is the same as {@link AbstractClusterListenerDistAddListenerTest#testNodeJoiningAndStateNodeDiesWithExistingClusterListener}.
    * This also has the twist of the fact that the node who dies is also has the cluster listener.  This test makes sure
    * that the subsequent node asked for cluster listeners hasn't yet got the view change and still has the cluster
    * listener in it.  Also the requesting node should have the view change before installing.
    * @throws TimeoutException
    * @throws InterruptedException
    * @throws ExecutionException
    */
   @Test(enabled = false, description = "Test may not be doable, check TODO in test")
   public void testNodeJoiningAndStateNodeDiesWhichHasClusterListener() throws TimeoutException,
                                                                               InterruptedException,
                                                                               ExecutionException {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);
      Cache<Object, String> cache2 = cache(2, CACHE_NAME);

      int initialCache0ListenerSize = cache0.getAdvancedCache().getListeners().size();
      int initialCache1ListenerSize = cache1.getAdvancedCache().getListeners().size();
      int initialCache2ListenerSize = cache2.getAdvancedCache().getListeners().size();

      ClusterListener clusterListener = new ClusterListener();
      cache0.addListener(clusterListener);

      assertEquals(cache0.getAdvancedCache().getListeners().size(), initialCache0ListenerSize + 1);
      assertEquals(cache1.getAdvancedCache().getListeners().size(), initialCache1ListenerSize + 1);
      assertEquals(cache2.getAdvancedCache().getListeners().size(), initialCache2ListenerSize + 1);

      // Make sure cache0 will  be the one will get the cluster listener request
      assertEquals(manager(0).getAddress(), manager(0).getMembers().get(0));

      CheckPoint checkPoint = new CheckPoint();

      waitUntilRequestingListeners(cache0, checkPoint);
      checkPoint.triggerForever("post_cluster_listeners_release_" + cache0);
      waitUntilViewChangeOccurs(manager(1), "manager1", checkPoint);

      // We let the first view change occur just fine on cache1 (this will be the addition of cache3).
      // What we want to block is the second one which is the removal of cache0
      checkPoint.trigger("pre_view_listener_release_" + "manager1");

      // First we add the new node, but block the dist exec execution
      log.info("Adding a new node ..");
      addClusterEnabledCacheManager(builderUsed);
      log.info("Added a new node");

      waitUntilViewChangeOccurs(manager(3), "manager3", checkPoint);
      // We don't want to block the view listener change on cache3
      checkPoint.trigger("pre_view_listener_release_" + "manager3");

      Future<Cache<Object, String>> future = fork(new Callable<Cache<Object, String>>() {
         @Override
         public Cache<Object, String> call() throws Exception {
            return cache(3, CACHE_NAME);
         }
      });

      // Wait for view change to occur on cache1 for the addition of cache3
      // Note we haven't triggered the view change for cache1 for the following removal yet
      checkPoint.awaitStrict("post_view_listener_invoked_" + "manager1", 10, TimeUnit.SECONDS);
      // Wait for the cluster listener request to come into cache0 which is from cache3
      checkPoint.awaitStrict("pre_cluster_listeners_invoked_" + cache0, 10, TimeUnit.SECONDS);

      // Now we kill cache0 while it is processing the request from cache3, which will in turn force it to ask cache1
      log.info("Killing node 0 ..");
      TestingUtil.killCacheManagers(manager(0));
      cacheManagers.remove(0);
      log.info("Node 0 killed");

      // TODO: need a away to verify that the response was sent back to cache3 before releasing the next line.  However
      // with no reference to cache I don't think this is possible.  If this can be fixed then test can be reenabled
      Cache<Object, String> cache3 = future.get(10, TimeUnit.SECONDS);

      // Now we can finally let the view change complete on cache1
      checkPoint.triggerForever("pre_view_listener_release_" + "manager1");

      // Now wait for cache3 to come up fully
      TestingUtil.blockUntilViewsReceived(60000, false, cache1, cache2);
      TestingUtil.waitForRehashToComplete(cache1, cache2);

      MagicKey key = new MagicKey(cache3);
      cache3.put(key, FIRST_VALUE);

      assertEquals(cache1.getAdvancedCache().getListeners().size(), initialCache1ListenerSize);
      assertEquals(cache2.getAdvancedCache().getListeners().size(), initialCache2ListenerSize);

      // Since we can't get a reliable start count, make sure no RemoteClusterListener is present which is added for
      // a cluster listener
      for (Object listener : cache3.getAdvancedCache().getListeners()) {
         assertFalse(listener instanceof RemoteClusterListener);
      }
   }

   protected void waitUntilRequestingListeners(final Cache<?, ?> cache, final CheckPoint checkPoint) {
      StateProvider sp = TestingUtil.extractComponent(cache, StateProvider.class);
      final Answer<Object> forwardedAnswer = AdditionalAnswers.delegatesTo(sp);
      StateProvider mockProvider = mock(StateProvider.class, withSettings().defaultAnswer(forwardedAnswer));
      doAnswer(new Answer() {
         @Override
         public Object answer(InvocationOnMock invocation) throws Throwable {
            // Wait for main thread to sync up
            checkPoint.trigger("pre_cluster_listeners_invoked_" + cache);
            // Now wait until main thread lets us through
            checkPoint.awaitStrict("pre_cluster_listeners_release_" + cache, 10, TimeUnit.SECONDS);

            try {
               return forwardedAnswer.answer(invocation);
            } finally {
               // Wait for main thread to sync up
               checkPoint.trigger("post_cluster_listeners_invoked_" + cache);
               // Now wait until main thread lets us through
               checkPoint.awaitStrict("post_cluster_listeners_release_" + cache, 10, TimeUnit.SECONDS);
            }
         }
      }).when(mockProvider).getClusterListenersToInstall();
      TestingUtil.replaceComponent(cache, StateProvider.class, mockProvider, true);
   }
}
