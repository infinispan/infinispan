package org.infinispan.xsite.irac;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.test.TestingUtil;
import org.infinispan.xsite.AbstractMultipleSitesTest;
import org.jgroups.UnreachableException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Functional test for exponential back-off with IRAC.
 * </p>
 * This test uses 3 sites with a cluster of size 1 for simplification. We issue all commands from site 1,
 * which has sites 2 and 3 as backups. The requests from 1 -> 2 will complete on the first try,
 * whereas 1 -> 3 will need the back-off to kick in.
 * </p>
 * We verify that the back-off only retries the failed operations. We have the same verifications as
 * {@link IracExponentialBackOffTest}.
 *
 * @author Jose Bolina
 * @since 15.0
 */
@Test(groups = "functional", testName = "xsite.irac.Irac3SitesExponentialBackOffTest")
public class Irac3SitesExponentialBackOffTest extends AbstractMultipleSitesTest {
   private static final int N_SITES = 3;
   private static final int CLUSTER_SIZE = 1;
   private static final Supplier<Throwable> NO_EXCEPTION = () -> null;
   private final Map<String, ControlledExponentialBackOff> backOffMap = new ConcurrentHashMap<>();
   private volatile ControlledTransport transport;


   @Override
   protected ConfigurationBuilder defaultConfigurationForSite(int siteIndex) {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      for (int i = 0; i < N_SITES; ++i) {
         if (i == siteIndex) {
            //don't add our site as backup.
            continue;
         }
         builder.sites()
               .addBackup()
               .site(siteName(i))
               .strategy(BackupConfiguration.BackupStrategy.ASYNC);
      }
      return builder;
   }

   @Override
   protected int defaultNumberOfSites() {
      return N_SITES;
   }

   @Override
   protected int defaultNumberOfNodes() {
      return CLUSTER_SIZE;
   }

   @Override
   protected void afterSitesCreated() {
      Cache<String, String> c = cache(siteName(0), 0);
      Collection<String> connected = Arrays.asList(siteName(0), siteName(1));
      Collection<String> disconnected = Collections.singletonList(siteName(2));

      transport = TestingUtil.wrapGlobalComponent(manager(c), Transport.class,
            actual -> new ControlledTransport(actual, siteName(0), connected, disconnected), true);
      DefaultIracManager iracManager = (DefaultIracManager) TestingUtil.extractComponent(c, IracManager.class);
      iracManager.setBackOff(backup -> backOffMap.computeIfAbsent(backup.getSiteName(), ControlledExponentialBackOff::new));
   }

   @AfterMethod(alwaysRun = true)
   public void resetStateAfterTest() {
      backOffMap.values().forEach(ControlledExponentialBackOff::release);

      Cache<String, String> c = cache(siteName(0), 0);
      DefaultIracManager iracManager = (DefaultIracManager) TestingUtil.extractComponent(c, IracManager.class);
      eventually(iracManager::isEmpty);
      backOffMap.values().forEach(ControlledExponentialBackOff::cleanupEvents);
      backOffMap.values().forEach(ControlledExponentialBackOff::assertNoEvents);
   }

   public void testSimulatedTimeout(Method method) {
      doTest(method, () -> log.requestTimedOut(1, siteName(2), "some time"));
   }

   public void testSimulatedUnreachableException(Method method) {
      doTest(method, () -> new UnreachableException(null));
   }

   public void testSiteUnreachable(Method method) {
      doTest(method, () -> log.remoteNodeSuspected(null));
   }

   public void testNoBackoffOnOtherException(Method method) {
      transport.throwableSupplier = CacheException::new;
      Cache<String, String> c = cache(siteName(0), 0);

      final String key = TestingUtil.k(method);
      final String value = TestingUtil.v(method);

      c.put(key, value);

      // Since no back off applied, both issues a reset. One backup succeeds and another fails.
      backOffMap.get(siteName(1)).eventually("Both reset with CacheException.", ControlledExponentialBackOff.Event.RESET);
      backOffMap.get(siteName(2)).eventually("Both reset with CacheException.", ControlledExponentialBackOff.Event.RESET);

      //with "normal" exception, the protocol will keep trying to send the request
      //we need to let it have a successful request otherwise it will fill the queue with RESET events.
      //it is possible that between the prev check and changing this, that already happened.
      transport.throwableSupplier = NO_EXCEPTION;

      DefaultIracManager iracManager = (DefaultIracManager) TestingUtil.extractComponent(c, IracManager.class);
      eventually(iracManager::isEmpty);

      // Only the backup that failed issued an event now, so only RESET event here (could be more than one).
      backOffMap.get(siteName(1)).assertNoEvents();
      backOffMap.get(siteName(2)).containsOnly("Only one that failed reset.", ControlledExponentialBackOff.Event.RESET);

      // Back off not applied.
      backOffMap.values().forEach(ControlledExponentialBackOff::assertNoEvents);
   }

   private void doTest(Method method, Supplier<Throwable> throwableSupplier) {
      Cache<String, String> c = cache(siteName(0), 0);
      transport.throwableSupplier = throwableSupplier;

      final String key = TestingUtil.k(method);
      final String value = TestingUtil.v(method);

      c.put(key, value);

      // With 2 backups, one succeeds and another fails.
      backOffMap.get(siteName(1)).eventually("Backoff event on first try.", ControlledExponentialBackOff.Event.RESET);
      backOffMap.get(siteName(2)).eventually("Backoff event on first try.", ControlledExponentialBackOff.Event.BACK_OFF);

      // Release will trigger the backoff to the failed site.
      backOffMap.get(siteName(2)).release();

      // Only one site sends the keys, so only a single event here.
      backOffMap.get(siteName(2)).eventually("Backoff event after release.", ControlledExponentialBackOff.Event.BACK_OFF);

      // Operation now should succeed.
      transport.throwableSupplier = NO_EXCEPTION;
      backOffMap.get(siteName(2)).release();

      // The operation finally succeeds, since only one backup was failing we have only one event.
      backOffMap.get(siteName(2)).eventually("All operations should succeed.", ControlledExponentialBackOff.Event.RESET);

      // No other event was issued.
      backOffMap.values().forEach(ControlledExponentialBackOff::assertNoEvents);
   }
}
