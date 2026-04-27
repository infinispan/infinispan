package org.infinispan.topology;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.testing.Exceptions.expectCompletionException;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.distribution.ch.impl.DefaultConsistentHashFactory;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.partitionhandling.impl.PreferAvailabilityStrategy;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.statetransfer.RebalanceType;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.util.logging.events.EventLogManager;
import org.infinispan.util.logging.events.TestingEventLogManager;
import org.mockito.Mockito;
import org.mockito.MockitoSession;
import org.mockito.quality.Strictness;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "topology.ClusterCacheStatusCapacityFactorTest")
public class ClusterCacheStatusCapacityFactorTest extends AbstractInfinispanTest {
   private static final String CACHE_NAME = "test";
   private static final CacheJoinInfo JOIN_INFO =
         new CacheJoinInfo(DefaultConsistentHashFactory.getInstance(), 8, 2, 1000,
               CacheMode.DIST_SYNC, 1.0f, null, Optional.empty());
   private static final Address A = Address.random("A");
   private static final Address B = Address.random("B");

   private ClusterCacheStatus status;
   private ClusterTopologyManagerImpl topologyManager;
   private InternalCacheRegistry internalCacheRegistry;
   private MockitoSession mockitoSession;

   @BeforeMethod(alwaysRun = true)
   public void setup() {
      mockitoSession = Mockito.mockitoSession().strictness(Strictness.LENIENT).startMocking();

      EventLogManager eventLogManager = new TestingEventLogManager();
      PersistentUUIDManager persistentUUIDManager = new PersistentUUIDManagerImpl();
      EmbeddedCacheManager cacheManager = mock(EmbeddedCacheManager.class);
      topologyManager = mock(ClusterTopologyManagerImpl.class);
      Transport transport = mock(Transport.class);
      GlobalComponentRegistry gcr = mock(GlobalComponentRegistry.class);
      internalCacheRegistry = mock(InternalCacheRegistry.class);
      when(gcr.getComponent(InternalCacheRegistry.class)).thenReturn(internalCacheRegistry);
      when(internalCacheRegistry.isInternalCache(CACHE_NAME)).thenReturn(false);
      when(topologyManager.isRebalancingEnabled()).thenReturn(true);

      PreferAvailabilityStrategy availabilityStrategy =
            new PreferAvailabilityStrategy(eventLogManager, persistentUUIDManager);
      status = new ClusterCacheStatus(cacheManager, gcr, CACHE_NAME, availabilityStrategy,
            RebalanceType.FOUR_PHASE, topologyManager, transport,
            persistentUUIDManager, eventLogManager,
            Optional.empty(), false);

      status.doJoin(A, makeJoinInfo(A));
      status.doJoin(B, makeJoinInfo(B));
   }

   @AfterMethod(alwaysRun = true)
   public void teardown() {
      mockitoSession.finishMocking();
   }

   public void testUpdateCapacityFactor() {
      assertThat(status.getCapacityFactors().get(B)).isEqualTo(1.0f);

      status.updateCapacityFactor(B, 0.5f);

      assertThat(status.getCapacityFactors().get(B)).isEqualTo(0.5f);
      assertThat(status.getCapacityFactors().get(A)).isEqualTo(1.0f);
   }

   public void testNodeNotMember() throws Exception {
      Address unknown = Address.random("unknown");
      Map<Address, Float> before = status.getCapacityFactors();

      CompletionStage<Void> stage = status.updateCapacityFactor(unknown, 0.5f);

      stage.toCompletableFuture().get(10, TimeUnit.SECONDS);
      assertThat(status.getCapacityFactors()).isSameAs(before);
   }

   public void testInternalCacheRejected() {
      when(internalCacheRegistry.isInternalCache(CACHE_NAME)).thenReturn(true);

      CompletionStage<Void> stage = status.updateCapacityFactor(A, 0.5f);

      expectCompletionException(IllegalStateException.class, stage);
   }

   public void testSameValueNoOp() throws Exception {
      Map<Address, Float> before = status.getCapacityFactors();

      CompletionStage<Void> stage = status.updateCapacityFactor(A, 1.0f);

      stage.toCompletableFuture().get(10, TimeUnit.SECONDS);
      assertThat(status.getCapacityFactors()).isSameAs(before);
   }

   public void testTotalCapacityZeroRejected() {
      status.updateCapacityFactor(A, 0f);
      assertThat(status.getCapacityFactors().get(A)).isEqualTo(0f);

      CompletionStage<Void> stage = status.updateCapacityFactor(B, 0f);

      expectCompletionException(IllegalArgumentException.class, stage);
      assertThat(status.getCapacityFactors().get(B)).isEqualTo(1.0f);
   }

   public void testCopyOnWriteCorrectness() {
      Map<Address, Float> before = status.getCapacityFactors();

      status.updateCapacityFactor(B, 0.5f);

      Map<Address, Float> after = status.getCapacityFactors();
      assertThat(after).isNotSameAs(before);
      assertThat(before.get(B)).isEqualTo(1.0f);
      assertThat(after.get(B)).isEqualTo(0.5f);
   }

   private CacheJoinInfo makeJoinInfo(Address a) {
      UUID persistentUUID = new UUID(a.hashCode(), a.hashCode());
      return new CacheJoinInfo(JOIN_INFO.getConsistentHashFactory(), JOIN_INFO.getNumSegments(),
            JOIN_INFO.getNumOwners(), JOIN_INFO.getTimeout(), JOIN_INFO.getCacheMode(),
            JOIN_INFO.getCapacityFactor(), persistentUUID, Optional.empty());
   }
}
