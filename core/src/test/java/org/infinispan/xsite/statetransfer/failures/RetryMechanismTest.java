package org.infinispan.xsite.statetransfer.failures;

import static org.infinispan.test.TestingUtil.wrapComponent;
import static org.infinispan.test.TestingUtil.wrapInboundInvocationHandler;
import static org.infinispan.xsite.statetransfer.XSiteStateTransferManager.STATUS_ERROR;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.Cache;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.BackupConfigurationBuilder;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.remoting.inboundhandler.AbstractDelegatingHandler;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.Reply;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.xsite.BackupReceiver;
import org.infinispan.xsite.BackupReceiverDelegator;
import org.infinispan.xsite.statetransfer.XSiteState;
import org.infinispan.xsite.statetransfer.XSiteStateConsumer;
import org.infinispan.xsite.statetransfer.XSiteStatePushCommand;
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
      takeSiteOffline();
      final Object key = new MagicKey(cache(NYC, 1));
      final FailureHandler handler = FailureHandler.replaceOn(cache(NYC, 1));
      final CounterBackupReceiver counterRepository = replaceBackupReceiverOn(cache(NYC, 0));

      cache(LON, 0).put(key, VALUE);

      handler.fail(); //it fails 3 times and then succeeds.

      startStateTransfer();
      assertOnline(LON, NYC);

      awaitXSiteStateSent(LON);
      assertEventuallyNoStateTransferInReceivingSite(null);

      assertEquals(0, handler.remainingFails());
      assertEquals(1, counterRepository.counter.get());
      assertInSite(NYC, cache -> assertEquals(VALUE, cache.get(key)));
   }

   /**
    * Simple scenario where the primary owner always throws an exception. The state transfer will not be successful
    * (test retry in NYC and LON).
    */
   public void testExceptionWithFailedRetry() {
      takeSiteOffline();
      final Object key = new MagicKey(cache(NYC, 1));
      final FailureHandler handler = FailureHandler.replaceOn(cache(NYC, 1));
      final CounterBackupReceiver counterRepository = replaceBackupReceiverOn(cache(NYC, 0));

      cache(LON, 0).put(key, VALUE);

      handler.failAlways();

      startStateTransfer();
      assertOnline(LON, NYC);

      awaitXSiteStateSent(LON);
      assertEventuallyNoStateTransferInReceivingSite(null);

      assertXSiteErrorStatus();

      assertEquals(3 /*max_retry + 1*/, counterRepository.counter.get());
      assertInSite(NYC, cache -> assertNull(cache.get(key)));
   }

   /**
    * Simple scenario where the primary owner leaves the cluster and the site master will apply the data locally (tests
    * retry in NYC, from remote to local).
    */
   public void testRetryLocally() throws ExecutionException, InterruptedException {
      takeSiteOffline();
      final Object key = new MagicKey(cache(NYC, 1));
      final DiscardHandler handler = DiscardHandler.replaceOn(cache(NYC, 1));
      final CounterBackupReceiver counterRepository = replaceBackupReceiverOn(cache(NYC, 0));

      cache(LON, 0).put(key, VALUE);

      startStateTransfer();
      assertOnline(LON, NYC);

      eventually(() -> handler.discarded);

      triggerTopologyChange(NYC, 1).get();

      awaitXSiteStateSent(LON);
      assertEventuallyNoStateTransferInReceivingSite(null);

      assertEquals(1, counterRepository.counter.get());

      assertInSite(NYC, cache -> assertEquals(VALUE, cache.get(key)));
   }

   /**
    * Simple scenario where the primary owner leaves the cluster and the NYC site master will apply the data locally.
    * The 1st and the 2nd time will fail and only the 3rd will succeed (testing local retry)
    */
   public void testMultipleRetryLocally() throws ExecutionException, InterruptedException {
      takeSiteOffline();
      final Object key = new MagicKey(cache(NYC, 1));
      final DiscardHandler handler = DiscardHandler.replaceOn(cache(NYC, 1));
      final FailureXSiteConsumer failureXSiteConsumer = FailureXSiteConsumer.replaceOn(cache(NYC, 0));
      final CounterBackupReceiver counterRepository = replaceBackupReceiverOn(cache(NYC, 0));

      failureXSiteConsumer.fail();

      cache(LON, 0).put(key, VALUE);

      startStateTransfer();
      assertOnline(LON, NYC);

      eventually(() -> handler.discarded);

      triggerTopologyChange(NYC, 1).get();

      awaitXSiteStateSent(LON);
      assertEventuallyNoStateTransferInReceivingSite(null);

      assertEquals(0, failureXSiteConsumer.remainingFails());

      assertEquals(1, counterRepository.counter.get());

      assertInSite(NYC, cache -> assertEquals(VALUE, cache.get(key)));
   }

   /**
    * Simple scenario where the primary owner leaves the cluster and the NYC site master will apply the data locally
    * (test retry in the LON site).
    */
   public void testFailRetryLocally() throws ExecutionException, InterruptedException {
      takeSiteOffline();
      final Object key = new MagicKey(cache(NYC, 1));
      final DiscardHandler handler = DiscardHandler.replaceOn(cache(NYC, 1));
      final FailureXSiteConsumer failureXSiteConsumer = FailureXSiteConsumer.replaceOn(cache(NYC, 0));
      final CounterBackupReceiver counterRepository = replaceBackupReceiverOn(cache(NYC, 0));

      failureXSiteConsumer.failAlways();

      cache(LON, 0).put(key, VALUE);

      startStateTransfer();
      assertOnline(LON, NYC);

      eventually(() -> handler.discarded);

      triggerTopologyChange(NYC, 1).get();

      awaitXSiteStateSent(LON);
      assertEventuallyNoStateTransferInReceivingSite(null);

      //tricky part. When the primary owners dies, the site master or the other node can become the primary owner
      //if the site master is enabled, it will never be able to apply the state (XSiteStateConsumer is throwing exception!)
      //otherwise, the other node will apply the state
      if (STATUS_ERROR.equals(getXSitePushStatus())) {
         assertEquals(3 /*max_retry + 1*/, counterRepository.counter.get());

         assertInSite(NYC, cache -> assertNull(cache.get(key)));
      } else {
         assertEquals(2 /*the 1st retry succeed*/, counterRepository.counter.get());
         assertInSite(NYC, cache -> assertEquals(VALUE, cache.get(key)));
      }
   }

   @Override
   protected void adaptLONConfiguration(BackupConfigurationBuilder builder) {
      super.adaptLONConfiguration(builder);
      builder.stateTransfer().maxRetries(2).waitTime(1000);
      builder.clustering().hash().numSegments(8); //we only use 1 key; no need for 256 segments
   }

   @Override
   protected ConfigurationBuilder getNycActiveConfig() {
      ConfigurationBuilder builder = super.getNycActiveConfig();
      builder.clustering().hash().numSegments(8); //we only use 1 key; no need for 256 segments
      return builder;
   }

   private static class FailureXSiteConsumer implements XSiteStateConsumer {

      static final int FAIL_FOR_EVER = -1;
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
      public void applyState(List<XSiteState> chunk) throws Exception {
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

      static FailureXSiteConsumer replaceOn(Cache<?, ?> cache) {
         return wrapComponent(cache, XSiteStateConsumer.class, (wrapOn, current) -> new FailureXSiteConsumer(current),
               true);
      }

      void fail() {
         synchronized (this) {
            this.nFailures = 3;
         }
      }

      void failAlways() {
         synchronized (this) {
            this.nFailures = FAIL_FOR_EVER;
         }
      }

      int remainingFails() {
         synchronized (this) {
            return nFailures;
         }
      }
   }

   private static class DiscardHandler extends AbstractDelegatingHandler {

      private volatile boolean discarded = false;

      private DiscardHandler(PerCacheInboundInvocationHandler delegate) {
         super(delegate);
      }

      static DiscardHandler replaceOn(Cache<?, ?> cache) {
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

      static final int FAIL_FOR_EVER = -1;

      //fail if > 0
      private int nFailures = 0;

      private FailureHandler(PerCacheInboundInvocationHandler delegate) {
         super(delegate);
      }

      static FailureHandler replaceOn(Cache<?, ?> cache) {
         return wrapInboundInvocationHandler(cache, FailureHandler::new);
      }

      void fail() {
         synchronized (this) {
            this.nFailures = 3;
         }
      }

      void failAlways() {
         synchronized (this) {
            this.nFailures = FAIL_FOR_EVER;
         }
      }

      int remainingFails() {
         synchronized (this) {
            return nFailures;
         }
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

   private static class CounterBackupReceiver extends BackupReceiverDelegator {

      private final AtomicInteger counter;

      CounterBackupReceiver(BackupReceiver delegate) {
         super(delegate);
         this.counter = new AtomicInteger();
      }

      @Override
      public CompletionStage<Void> handleStateTransferState(List<XSiteState> chunk, long timeoutMs) {
         counter.getAndIncrement();
         return super.handleStateTransferState(chunk, timeoutMs);
      }
   }

   private static CounterBackupReceiver replaceBackupReceiverOn(Cache<?,?> cache) {
      return wrapComponent(cache, BackupReceiver.class, CounterBackupReceiver::new);
   }
}
