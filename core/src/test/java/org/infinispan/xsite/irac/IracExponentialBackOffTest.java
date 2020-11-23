package org.infinispan.xsite.irac;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.AbstractDelegatingTransport;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.XSiteResponse;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.ExponentialBackOff;
import org.infinispan.xsite.XSiteBackup;
import org.infinispan.xsite.XSiteReplicateCommand;
import org.jgroups.UnreachableException;
import org.testng.annotations.Test;

/**
 * Function test for exponential back-off with IRAC.
 * <p>
 * It tests if the {@link DefaultIracManager} respects the exception and invokes the proper {@link ExponentialBackOff}
 * methods.
 *
 * @author Pedro Ruivo
 * @since 12.0
 */
@Test(groups = "functional", testName = "xsite.iract.IracExponentialBackOffTest")
public class IracExponentialBackOffTest extends SingleCacheManagerTest {

   private static final String LON = "LON";
   private static final String NYC = "NYC";
   private static final String CACHE_NAME = "irac-exponential-backoff";
   private static final Supplier<Throwable> NO_EXCEPTION = () -> null;
   private final ControlledExponentialBackOff backOff = new ControlledExponentialBackOff();
   private volatile ControlledTransport transport;
   private volatile DefaultIracManager iracManager;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      //default cache manager
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createClusteredCacheManager();
      this.transport = TestingUtil.wrapGlobalComponent(cacheManager, Transport.class, ControlledTransport::new, true);
      this.cache = cacheManager.administration()
            .withFlags(CacheContainerAdmin.AdminFlag.VOLATILE)
            .getOrCreateCache(CACHE_NAME, createCacheConfiguration().build());
      iracManager = (DefaultIracManager) TestingUtil.extractComponent(cache, IracManager.class);
      iracManager.setBackOff(backOff);
      return cacheManager;
   }

   private static ConfigurationBuilder createCacheConfiguration() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC);
      builder.sites().addBackup()
            .site(NYC)
            .strategy(BackupConfiguration.BackupStrategy.ASYNC);
      return builder;
   }

   public void testSimulatedTimeout(Method method) throws InterruptedException {
      doTest(method, () -> log.requestTimedOut(1, NYC));
   }

   public void testSimulatedUnreachableException(Method method) throws InterruptedException {
      doTest(method, () -> new UnreachableException(null));
   }

   public void testSimulatedSiteUnreachableEvent(Method method) throws InterruptedException {
      doTest(method, () -> log.remoteNodeSuspected(null));
   }

   public void testNoBackoffOnOtherException(Method method) throws InterruptedException {
      backOff.drainPermits();
      backOff.cleanupEvents();
      backOff.assertNoEvents();
      transport.throwableSupplier = CacheException::new;

      final String key = TestingUtil.k(method);
      final String value = TestingUtil.v(method);

      cache.put(key, value);

      backOff.eventually("Reset event with CacheException.", Event.RESET);

      //with "normal" exception, the protocol will keep trying to send the request
      //we need to let it have a successful request otherwise it will fill queue with RESET events.
      transport.throwableSupplier = NO_EXCEPTION;
      eventually(iracManager::isEmpty);
      backOff.cleanupEvents();
      backOff.assertNoEvents();
   }

   private void doTest(Method method, Supplier<Throwable> throwableSupplier) throws InterruptedException {
      backOff.drainPermits();
      backOff.cleanupEvents();
      backOff.assertNoEvents();
      transport.throwableSupplier = throwableSupplier;

      final String key = TestingUtil.k(method);
      final String value = TestingUtil.v(method);

      cache.put(key, value);

      backOff.eventually("Backoff event on first try.", Event.BACK_OFF);

      //the release should trigger another back off event
      backOff.release();
      backOff.eventually("Backoff event on second try.", Event.BACK_OFF);

      //no exception, request will be completed.
      transport.throwableSupplier = NO_EXCEPTION;
      backOff.release();

      eventually(iracManager::isEmpty);
      backOff.eventually("Reset event after successful try", Event.RESET);
      backOff.assertNoEvents();
   }

   private static class ControlledExponentialBackOff implements ExponentialBackOff {

      private final BlockingDeque<Event> backOffEvents;
      private final Semaphore semaphore;
      private volatile CompletableFuture<Void> backOff = new CompletableFuture<>();

      private ControlledExponentialBackOff() {
         backOffEvents = new LinkedBlockingDeque<>();
         semaphore = new Semaphore(0);
      }

      @Override
      public void reset() {
         backOffEvents.add(Event.RESET);
      }

      @Override
      public CompletionStage<Void> asyncBackOff() {
         backOffEvents.add(Event.BACK_OFF);
         return backOff;
      }

      void release() {
         semaphore.release(1);
         backOff.complete(null);
         this.backOff = new CompletableFuture<>();
      }

      void drainPermits() {
         semaphore.drainPermits();
         backOff.complete(null);
         this.backOff = new CompletableFuture<>();
      }

      void cleanupEvents() {
         backOffEvents.clear();
      }

      void eventually(String message, Event expected) throws InterruptedException {
         Event current = backOffEvents.poll(30, TimeUnit.SECONDS);
         assertEquals(message, expected, current);
      }

      void assertNoEvents() {
         assertTrue("Expected no events.", backOffEvents.isEmpty());
      }
   }

   static class ControlledTransport extends AbstractDelegatingTransport {

      private volatile Supplier<Throwable> throwableSupplier = NO_EXCEPTION;

      ControlledTransport(Transport actual) {
         super(actual);
      }

      @Override
      public void start() {
         //already started
      }

      @Override
      public <O> XSiteResponse<O> backupRemotely(XSiteBackup backup, XSiteReplicateCommand<O> rpcCommand) {
         ControlledXSiteResponse<O> response = new ControlledXSiteResponse<>(backup, throwableSupplier.get());
         response.complete();
         return response;
      }

      @Override
      public void checkCrossSiteAvailable() throws CacheConfigurationException {
         //no-op == it is available
      }

      @Override
      public String localSiteName() {
         return LON;
      }

      @Override
      public Set<String> getSitesView() {
         return Collections.singleton(LON);
      }
   }

   private static class ControlledXSiteResponse<T> extends CompletableFuture<T> implements XSiteResponse<T> {

      private final XSiteBackup backup;
      private final Throwable result;

      private ControlledXSiteResponse(XSiteBackup backup, Throwable result) {
         this.backup = backup;
         this.result = result;
      }

      @Override
      public void whenCompleted(XSiteResponseCompleted listener) {
         listener.onCompleted(backup, System.currentTimeMillis(), 0, result);
      }

      void complete() {
         if (result == null) {
            complete(null);
         } else {
            completeExceptionally(result);
         }
      }
   }

   private enum Event {
      BACK_OFF,
      RESET
   }

}
