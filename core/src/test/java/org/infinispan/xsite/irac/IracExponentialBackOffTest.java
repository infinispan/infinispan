package org.infinispan.xsite.irac;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.function.Supplier;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.ExponentialBackOff;
import org.jgroups.UnreachableException;
import org.testng.annotations.AfterMethod;
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
      this.transport = TestingUtil.wrapGlobalComponent(cacheManager, Transport.class,
            actual -> new ControlledTransport(actual, LON, Collections.singleton(NYC)), true);
      this.cache = cacheManager.administration()
            .withFlags(CacheContainerAdmin.AdminFlag.VOLATILE)
            .getOrCreateCache(CACHE_NAME, createCacheConfiguration().build());
      iracManager = (DefaultIracManager) TestingUtil.extractComponent(cache, IracManager.class);
      iracManager.setBackOff(backup -> backOff);
      return cacheManager;
   }

   @AfterMethod(alwaysRun = true)
   public void resetStateAfterTest() {
      backOff.release();
      eventually(iracManager::isEmpty);
      backOff.cleanupEvents();
      backOff.assertNoEvents();
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
      doTest(method, () -> log.requestTimedOut(1, NYC, "some time"));
   }

   public void testSimulatedUnreachableException(Method method) throws InterruptedException {
      doTest(method, () -> new UnreachableException(null));
   }

   public void testSimulatedSiteUnreachableEvent(Method method) throws InterruptedException {
      doTest(method, () -> log.remoteNodeSuspected(null));
   }

   public void testNoBackoffOnOtherException(Method method) throws InterruptedException {
      transport.throwableSupplier = CacheException::new;

      final String key = TestingUtil.k(method);
      final String value = TestingUtil.v(method);

      cache.put(key, value);

      backOff.eventually("Reset event with CacheException.", ControlledExponentialBackOff.Event.RESET);

      //with "normal" exception, the protocol will keep trying to send the request
      //we need to let it have a successful request otherwise it will fill queue with RESET events.
      transport.throwableSupplier = NO_EXCEPTION;
      eventually(iracManager::isEmpty);
      backOff.cleanupEvents();
      backOff.assertNoEvents();
   }

   private void doTest(Method method, Supplier<Throwable> throwableSupplier) throws InterruptedException {
      transport.throwableSupplier = throwableSupplier;

      final String key = TestingUtil.k(method);
      final String value = TestingUtil.v(method);

      cache.put(key, value);

      backOff.eventually("Backoff event on first try.", ControlledExponentialBackOff.Event.BACK_OFF);

      //the release should trigger another back off event
      backOff.release();
      backOff.eventually("Backoff event on second try.", ControlledExponentialBackOff.Event.BACK_OFF);

      //no exception, request will be completed.
      transport.throwableSupplier = NO_EXCEPTION;
      backOff.release();

      eventually(iracManager::isEmpty);
      //backOff.eventually("Reset event after successful try operations", ControlledExponentialBackOff.Event.RESET);
      backOff.eventually("Reset event after successful try", ControlledExponentialBackOff.Event.RESET);
      backOff.assertNoEvents();
   }
}
