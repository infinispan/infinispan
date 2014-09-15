package org.infinispan.cdi.test.event;

import org.infinispan.AdvancedCache;
import org.infinispan.cdi.test.DefaultTestEmbeddedCacheManagerProducer;
import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.context.impl.NonTxInvocationContext;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheTopology;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.testng.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.util.Arrays;

import static org.infinispan.cdi.test.assertions.ObserverAssertion.assertThat;
import static org.infinispan.cdi.test.testutil.Deployments.baseDeployment;
import static org.mockito.Mockito.mock;

/**
 * Tests if event mechanism works correctly in Weld implementation (with Arquillian).
 * <p>
 *    Note this class depends indirectly on configuration provided in {@link org.infinispan.cdi.test.event.Config}
 *    class.
 * </p>
 *
 * @author Pete Muir
 * @author Sebastian Laskawiec
 * @see Config
 */
@Test(groups = "functional", testName = "cdi.test.event.CacheEventTest")
public class CacheEventTest extends Arquillian {

   private final NonTxInvocationContext invocationContext = new NonTxInvocationContext(AnyEquivalence.getInstance());

   @Inject
   @Cache1
   private AdvancedCache<String, String> cache1;

   @Inject
   @Cache2
   private AdvancedCache<String, String> cache2;

   @Inject
   private CacheObserver cacheObserver;

   private CacheNotifier<String, String> cache1Notifier;

   private CacheManagerNotifier cache1ManagerNotifier;

   @Deployment
   public static Archive<?> deployment() {
      return baseDeployment()
            .addPackage(CacheEventTest.class.getPackage())
            .addClass(DefaultTestEmbeddedCacheManagerProducer.class);
   }

   @AfterMethod
   public void afterMethod() {
      cache1.clear();
      cache2.clear();
      cacheObserver.clear();
   }

   @BeforeMethod
   public void beforeMethod() {
      cache1Notifier =  cache1.getComponentRegistry().getComponent(CacheNotifier.class);
      cache1ManagerNotifier = cache1.getComponentRegistry().getComponent(CacheManagerNotifier.class);
   }

   public void testFiringStartedEventOnNewlyStartedCache() throws Exception {
      //when
      cache1.stop();
      cache1.start();

      //then
      assertThat(cacheObserver, Cache1.class).hasProperName("cache1").hasStartedEvent();
   }

   public void testFiringStoppedEventWhenStoppingCache() throws Exception {
      //when
      cache1.stop();
      cache1.start();

      //then
      assertThat(cacheObserver, Cache1.class).hasProperName("cache1").hasStoppedEvent();
   }

   public void testFiringEntryCreatedEventWhenPuttingDataIntoCache() throws Exception {
      //when
      cache1.put("pete", "Edinburgh");

      //then
      assertThat(cacheObserver, Cache1.class).hasEntryCreatedEvent("pete");
   }

   public void testFiringEntryRemovedEventWhenRemovingDataFromCache() throws Exception {
      //given
      cache1.put("pete", "Edinburgh");

      //when
      cache1.remove("pete");

      //then
      assertThat(cacheObserver, Cache1.class).hasEntryRemovedEvent("pete");
   }

   public void testFiringEntryActivatedEventWhenUsingCacheNotifier() throws Exception {
      //when
      cache1Notifier.notifyCacheEntryActivated("pete", "Edinburgh", true, invocationContext, null);

      //then
      assertThat(cacheObserver, Cache1.class).hasEntryActivatedEvent("pete");
   }

   public void testFiringEntriesEvictedWhenEvictingDataInCache() throws Exception {
      //given
      cache1.put("pete", "Edinburgh");

      //when
      cache1.evict("pete");

      //then
      assertThat(cacheObserver, Cache1.class).hasEntriesEvictedEvent("pete");
   }

   public void testFiringEntryEvictedWhenEvictingDataInCache() throws Exception {
      //given
      cache1.put("pete", "Edinburgh");

      //when
      cache1.evict("pete");

      //then
      assertThat(cacheObserver, Cache1.class).hasEntryEvictedEvent("pete");
   }

   public void testFiringEntryModifiedEventWhenModifyingEntryInCache() throws Exception {
      //given
      cache1.put("pete", "Edinburgh");

      //when
      cache1.put("pete", "Edinburgh2");

      //then
      assertThat(cacheObserver, Cache1.class).hasEntryModifiedEvent("pete");
   }

   public void testFiringEntryInvalidatedWhenUsingCacheNotifier() throws Exception {
      //when
      cache1Notifier.notifyCacheEntryInvalidated("pete", "Edinburgh", true, invocationContext, null);

      //then
      assertThat(cacheObserver, Cache1.class).hasEntryInvalidatedEvent("pete");
   }

   public void testFiringEntryLoadedWhenUsingCacheNotifier() throws Exception {
      //when
      cache1Notifier.notifyCacheEntryLoaded("pete", "Edinburgh", true, invocationContext, null);

      //then
      assertThat(cacheObserver, Cache1.class).hasEntryLoadedEvent("pete");
   }

   public void testFiringEntryPassivatedWhenUsingCacheNotifier() throws Exception {
      //when
      cache1Notifier.notifyCacheEntryPassivated("pete", "Edinburgh", true, invocationContext, null);

      //then
      assertThat(cacheObserver, Cache1.class).hasEntryPassivatedEvent("pete");
   }

   public void testFiringEntryVisitedWhenUsingCacheNotifier() throws Exception {
      //when
      cache1Notifier.notifyCacheEntryVisited("pete", "Edinburgh", true, invocationContext, null);

      //then
      assertThat(cacheObserver, Cache1.class).hasEntryVisitedEvent("pete");
   }

   public void testFiringDataRehashedWhenUsingCacheNotifier() throws Exception {
      //given
      ConsistentHash mockOldHash = mock(ConsistentHash.class);
      ConsistentHash mockNewHash = mock(ConsistentHash.class);
      ConsistentHash mockUnionHash = mock(ConsistentHash.class);

      //when
      cache1Notifier.notifyDataRehashed(mockOldHash, mockNewHash, mockUnionHash, 0, true);

      //then
      assertThat(cacheObserver, Cache1.class).hasDataRehashEvent(mockNewHash);
   }

   public void testFiringTransactionCompletedWhenUsingCacheNotifier() throws Exception {
      //given
      GlobalTransaction mockGlobalTransaction = mock(GlobalTransaction.class);

      //when
      cache1Notifier.notifyTransactionCompleted(mockGlobalTransaction, true, invocationContext);

      //then
      assertThat(cacheObserver, Cache1.class).hasTransactionCompletedEvent(true);
   }

   public void testFiringTransactionRegisteredWhenUsingCacheNotifier() throws Exception {
      //given
      GlobalTransaction mockGlobalTransaction = mock(GlobalTransaction.class);

      //when
      cache1Notifier.notifyTransactionRegistered(mockGlobalTransaction, true);

      //then
      assertThat(cacheObserver, Cache1.class).hasTransactionRegisteredEvent(true);
   }

   public void testFiringViewChangedWhenUsingCacheManagerNotifier() throws Exception {
      //given
      Address mockMyAddress = mock(Address.class);

      //when
      cache1ManagerNotifier.notifyViewChange(Arrays.asList(mockMyAddress), Arrays.asList(mockMyAddress), mockMyAddress, 0);

      //then
      assertThat(cacheObserver, Cache1.class).hasViewChangedEvent(mockMyAddress);
   }

   public void testFiringTopologyChangedWhenUsingCacheManagerNotifier() throws Exception {
      //given
      CacheTopology mockCacheTopology = mock(CacheTopology.class);

      //when
      cache1Notifier.notifyTopologyChanged(mockCacheTopology, mockCacheTopology, 0, true);

      //then
      assertThat(cacheObserver, Cache1.class).hasTopologyChangedEvent(0);
   }

   public void testSendingEventToProperEventObservers() throws Exception {
      //given
      cache1.put("cache1", "for cache1");
      cache2.put("cache2", "for cache2");

      //when
      cache1.remove("cache1");

      //then
      assertThat(cacheObserver, Cache1.class).hasEntryCreatedEvent("cache1").hasEntryRemovedEvent("cache1");
      assertThat(cacheObserver, Cache2.class).hasEntryCreatedEvent("cache2");
   }
}
