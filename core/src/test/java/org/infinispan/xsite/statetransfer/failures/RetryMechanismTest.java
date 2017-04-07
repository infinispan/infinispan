package org.infinispan.xsite.statetransfer.failures;

import static org.infinispan.test.TestingUtil.WrapFactory;
import static org.infinispan.test.TestingUtil.extractGlobalComponent;
import static org.infinispan.test.TestingUtil.replaceComponent;
import static org.infinispan.test.TestingUtil.wrapInboundInvocationHandler;
import static org.infinispan.test.TestingUtil.wrapComponent;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.Cache;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.BackupConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.manager.CacheContainer;
import org.infinispan.remoting.inboundhandler.AbstractDelegatingHandler;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.Reply;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.xsite.BackupReceiver;
import org.infinispan.xsite.BackupReceiverDelegator;
import org.infinispan.xsite.BackupReceiverRepository;
import org.infinispan.xsite.BackupReceiverRepositoryDelegator;
import org.infinispan.xsite.statetransfer.XSiteState;
import org.infinispan.xsite.statetransfer.XSiteStateConsumer;
import org.infinispan.xsite.statetransfer.XSiteStatePushCommand;
import org.infinispan.xsite.statetransfer.XSiteStateTransferManager;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

/**
 * Tests the multiple retry mechanism implemented in Cross-Site replication state transfer.
 *
 * @author Pedro Ruivo
 * @since 7.1
 */
@Test(groups = "xsite", testName = "xsite.statetransfer.failures.RetryMechanismTest")
public class RetryMechanismTest extends AbstractTopologyChangeTest {

   private static final String VALUE = "value";

   /**
    * Simple scenario where the primary owner throws an exception. The NYC site master retries and at the 3rd retry, it
    * will apply the data (test retry in NYC).
    */
   public void testExceptionWithSuccessfulRetry() {
      takeSiteOffline(LON, NYC);
      final Object key = new MagicKey(cache(NYC, 1));
      final FailureHandler handler = FailureHandler.replaceOn(cache(NYC, 1));
      final CounterBackupReceiverRepository counterRepository = CounterBackupReceiverRepository.replaceOn(cache(NYC, 0).getCacheManager());

      cache(LON, 0).put(key, VALUE);

      handler.fail(3); //it fails 3 times and then succeeds.

      startStateTransfer(cache(LON, 0), NYC);
      assertOnline(LON, NYC);

      awaitXSiteStateSent(LON);
      awaitXSiteStateReceived(NYC);

      AssertJUnit.assertEquals(0, handler.remainingFails());
      AssertJUnit.assertEquals(1, counterRepository.counter.get());
      assertInSite(NYC, new AssertCondition<Object, Object>() {
         @Override
         public void assertInCache(Cache<Object, Object> cache) {
            AssertJUnit.assertEquals(VALUE, cache.get(key));
         }
      });
   }

   /**
    * Simple scenario where the primary owner always throws an exception. The state transfer will not be successful
    * (test retry in NYC and LON).
    */
   public void testExceptionWithFailedRetry() {
      takeSiteOffline(LON, NYC);
      final Object key = new MagicKey(cache(NYC, 1));
      final FailureHandler handler = FailureHandler.replaceOn(cache(NYC, 1));
      final CounterBackupReceiverRepository counterRepository = CounterBackupReceiverRepository.replaceOn(cache(NYC, 0).getCacheManager());

      cache(LON, 0).put(key, VALUE);

      handler.failAlways();

      startStateTransfer(cache(LON, 0), NYC);
      assertOnline(LON, NYC);

      awaitXSiteStateSent(LON);
      awaitXSiteStateReceived(NYC);

      assertXSiteStatus(LON, NYC, XSiteStateTransferManager.STATUS_ERROR);

      AssertJUnit.assertEquals(3 /*max_retry + 1*/, counterRepository.counter.get());
      assertInSite(NYC, new AssertCondition<Object, Object>() {
         @Override
         public void assertInCache(Cache<Object, Object> cache) {
            AssertJUnit.assertNull(cache.get(key));
         }
      });
   }

   /**
    * Simple scenario where the primary owner leaves the cluster and the site master will apply the data locally (tests
    * retry in NYC, from remote to local).
    */
   public void testRetryLocally() throws ExecutionException, InterruptedException {
      takeSiteOffline(LON, NYC);
      final Object key = new MagicKey(cache(NYC, 1));
      final DiscardHandler handler = DiscardHandler.replaceOn(cache(NYC, 1));
      final CounterBackupReceiverRepository counterRepository = CounterBackupReceiverRepository.replaceOn(cache(NYC, 0).getCacheManager());

      cache(LON, 0).put(key, VALUE);

      startStateTransfer(cache(LON, 0), NYC);
      assertOnline(LON, NYC);

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return handler.discarded;
         }
      });

      triggerTopologyChange(NYC, 1).get();

      awaitXSiteStateSent(LON);
      awaitXSiteStateReceived(NYC);

      AssertJUnit.assertEquals(1, counterRepository.counter.get());

      assertInSite(NYC, new AssertCondition<Object, Object>() {
         @Override
         public void assertInCache(Cache<Object, Object> cache) {
            AssertJUnit.assertEquals(VALUE, cache.get(key));
         }
      });
   }

   /**
    * Simple scenario where the primary owner leaves the cluster and the NYC site master will apply the data locally.
    * The 1st and the 2nd time will fail and only the 3rd will succeed (testing local retry)
    */
   public void testMultipleRetryLocally() throws ExecutionException, InterruptedException {
      takeSiteOffline(LON, NYC);
      final Object key = new MagicKey(cache(NYC, 1));
      final DiscardHandler handler = DiscardHandler.replaceOn(cache(NYC, 1));
      final FailureXSiteConsumer failureXSiteConsumer = FailureXSiteConsumer.replaceOn(cache(NYC, 0));
      final CounterBackupReceiverRepository counterRepository = CounterBackupReceiverRepository.replaceOn(cache(NYC, 0).getCacheManager());

      failureXSiteConsumer.fail(3);

      cache(LON, 0).put(key, VALUE);

      startStateTransfer(cache(LON, 0), NYC);
      assertOnline(LON, NYC);

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return handler.discarded;
         }
      });

      triggerTopologyChange(NYC, 1).get();

      awaitXSiteStateSent(LON);
      awaitXSiteStateReceived(NYC);

      AssertJUnit.assertEquals(0, failureXSiteConsumer.remainingFails());

      AssertJUnit.assertEquals(1, counterRepository.counter.get());

      assertInSite(NYC, new AssertCondition<Object, Object>() {
         @Override
         public void assertInCache(Cache<Object, Object> cache) {
            AssertJUnit.assertEquals(VALUE, cache.get(key));
         }
      });
   }

   /**
    * Simple scenario where the primary owner leaves the cluster and the NYC site master will apply the data locally
    * (test retry in the LON site).
    */
   public void testFailRetryLocally() throws ExecutionException, InterruptedException {
      takeSiteOffline(LON, NYC);
      final Object key = new MagicKey(cache(NYC, 1));
      final DiscardHandler handler = DiscardHandler.replaceOn(cache(NYC, 1));
      final FailureXSiteConsumer failureXSiteConsumer = FailureXSiteConsumer.replaceOn(cache(NYC, 0));
      final CounterBackupReceiverRepository counterRepository = CounterBackupReceiverRepository.replaceOn(cache(NYC, 0).getCacheManager());

      failureXSiteConsumer.failAlways();

      cache(LON, 0).put(key, VALUE);

      startStateTransfer(cache(LON, 0), NYC);
      assertOnline(LON, NYC);

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return handler.discarded;
         }
      });

      triggerTopologyChange(NYC, 1).get();

      awaitXSiteStateSent(LON);
      awaitXSiteStateReceived(NYC);

      //tricky part. When the primary owners dies, the site master or the other node can become the primary owner
      //if the site master is enabled, it will never be able to apply the state (XSiteStateConsumer is throwing exception!)
      //otherwise, the other node will apply the state
      if (XSiteStateTransferManager.STATUS_ERROR.equals(getXSitePushStatus(LON, NYC))) {
         AssertJUnit.assertEquals(3 /*max_retry + 1*/, counterRepository.counter.get());

         assertInSite(NYC, new AssertCondition<Object, Object>() {
            @Override
            public void assertInCache(Cache<Object, Object> cache) {
               AssertJUnit.assertNull(cache.get(key));
            }
         });
      } else {
         AssertJUnit.assertEquals(2 /*the 1st retry succeed*/, counterRepository.counter.get());
         assertInSite(NYC, new AssertCondition<Object, Object>() {
            @Override
            public void assertInCache(Cache<Object, Object> cache) {
               AssertJUnit.assertEquals(VALUE, cache.get(key));
            }
         });
      }
   }

   @Override
   protected void adaptLONConfiguration(BackupConfigurationBuilder builder) {
      super.adaptLONConfiguration(builder);
      builder.stateTransfer().maxRetries(2).waitTime(1000);
   }

   private static class CounterBackupReceiverRepository extends BackupReceiverRepositoryDelegator {

      private final AtomicInteger counter;

      private CounterBackupReceiverRepository(BackupReceiverRepository delegate) {
         super(delegate);
         this.counter = new AtomicInteger();
      }

      @Override
      public BackupReceiver getBackupReceiver(String originSiteName, String cacheName) {
         return new BackupReceiverDelegator(super.getBackupReceiver(originSiteName, cacheName)) {
            @Override
            public void handleStateTransferState(XSiteStatePushCommand cmd) throws Exception {
               counter.getAndIncrement();
               super.handleStateTransferState(cmd);
            }
         };
      }

      public static CounterBackupReceiverRepository replaceOn(CacheContainer cacheContainer) {
         BackupReceiverRepository delegate = extractGlobalComponent(cacheContainer, BackupReceiverRepository.class);
         CounterBackupReceiverRepository wrapper = new CounterBackupReceiverRepository(delegate);
         replaceComponent(cacheContainer, BackupReceiverRepository.class, wrapper, true);
         return wrapper;
      }
   }

   private static class FailureXSiteConsumer implements XSiteStateConsumer {

      public static int FAIL_FOR_EVER = -1;
      private final XSiteStateConsumer delegate;
      //fail if > 0
      private int nFailures = 0;

      private FailureXSiteConsumer(XSiteStateConsumer delegate) {
         this.delegate = delegate;
      }

      @Override
      public void startStateTransfer(String sendingSite) {
         delegate.startStateTransfer(sendingSite);
      }

      @Override
      public void endStateTransfer(String sendingSite) {
         delegate.endStateTransfer(sendingSite);
      }

      @Override
      public void applyState(XSiteState[] chunk) throws Exception {
         boolean fail;
         synchronized (this) {
            fail = nFailures == FAIL_FOR_EVER;
            if (nFailures > 0) {
               fail = true;
               nFailures--;
            }
         }
         if (fail) {
            throw new CacheException("Induced Fail");
         }
         delegate.applyState(chunk);
      }

      @Override
      public String getSendingSiteName() {
         return delegate.getSendingSiteName();
      }

      public void fail(int nTimes) {
         if (nTimes < 0) {
            throw new IllegalArgumentException("nTimes should greater than zero but it is " + nTimes);
         }
         synchronized (this) {
            this.nFailures = nTimes;
         }
      }

      public void failAlways() {
         synchronized (this) {
            this.nFailures = FAIL_FOR_EVER;
         }
      }

      public int remainingFails() {
         synchronized (this) {
            return nFailures;
         }
      }

      public static FailureXSiteConsumer replaceOn(Cache<?, ?> cache) {
         return wrapComponent(cache, XSiteStateConsumer.class, new WrapFactory<XSiteStateConsumer, FailureXSiteConsumer, Cache<?, ?>>() {
            @Override
            public FailureXSiteConsumer wrap(Cache<?, ?> wrapOn, XSiteStateConsumer current) {
               return new FailureXSiteConsumer(current);
            }
         }, true);
      }
   }

   private static class DiscardHandler extends AbstractDelegatingHandler {

      private volatile boolean discarded = false;

      private DiscardHandler(PerCacheInboundInvocationHandler delegate) {
         super(delegate);
      }

      public static DiscardHandler replaceOn(Cache<?, ?> cache) {
         return wrapInboundInvocationHandler(cache, DiscardHandler::new);
      }

      @Override
      protected boolean beforeHandle(CacheRpcCommand command, Reply reply, DeliverOrder order) {
         if (!discarded) {
            discarded = command instanceof XSiteStatePushCommand;
         }
         return !discarded;
      }
   }

   private static class FailureHandler extends AbstractDelegatingHandler {

      public static int FAIL_FOR_EVER = -1;

      //fail if > 0
      private int nFailures = 0;

      private FailureHandler(PerCacheInboundInvocationHandler delegate) {
         super(delegate);
      }

      public void fail(int nTimes) {
         if (nTimes < 0) {
            throw new IllegalArgumentException("nTimes should greater than zero but it is " + nTimes);
         }
         synchronized (this) {
            this.nFailures = nTimes;
         }
      }

      public void failAlways() {
         synchronized (this) {
            this.nFailures = FAIL_FOR_EVER;
         }
      }

      public int remainingFails() {
         synchronized (this) {
            return nFailures;
         }
      }

      public static FailureHandler replaceOn(Cache<?, ?> cache) {
         return wrapInboundInvocationHandler(cache, FailureHandler::new);
      }

      @Override
      protected synchronized boolean beforeHandle(CacheRpcCommand command, Reply reply, DeliverOrder order) {
         if (command instanceof XSiteStatePushCommand) {
            boolean fail;
            synchronized (this) {
               fail = nFailures == FAIL_FOR_EVER;
               if (nFailures > 0) {
                  fail = true;
                  nFailures--;
               }
            }
            if (fail) {
               reply.reply(new ExceptionResponse(new CacheException("Induced Fail.")));
               return false;
            }
         }
         return true;
      }
   }


}
