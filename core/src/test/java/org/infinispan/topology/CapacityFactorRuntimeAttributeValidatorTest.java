package org.infinispan.topology;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.topology.CapacityFactorUpdateCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ClusteringConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.TransportConfiguration;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.test.AbstractInfinispanTest;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.MockitoSession;
import org.mockito.quality.Strictness;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "topology.CapacityFactorRuntimeAttributeValidatorTest")
public class CapacityFactorRuntimeAttributeValidatorTest extends AbstractInfinispanTest {

   private static final String CACHE_NAME = "test";

   private MockitoSession mockitoSession;
   private TopologyManagementHelper helper;
   private Transport transport;
   private Address address;

   @BeforeMethod(alwaysRun = true)
   public void setup() {
      mockitoSession = Mockito.mockitoSession().strictness(Strictness.LENIENT).startMocking();
      helper = mock(TopologyManagementHelper.class);
      transport = mock(Transport.class);
      address = Address.random("A");
      when(transport.getAddress()).thenReturn(address);
      when(helper.executeOnCoordinator(any(), any(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(null));
   }

   @AfterMethod(alwaysRun = true)
   public void teardown() {
      mockitoSession.finishMocking();
   }

   private CapacityFactorRuntimeAttributeValidator createValidator(CacheMode cacheMode, boolean zeroCapacityNode) {
      GlobalConfiguration globalConfig = mock(GlobalConfiguration.class);
      when(globalConfig.isZeroCapacityNode()).thenReturn(zeroCapacityNode);

      TransportConfiguration transportConfiguration = mock(TransportConfiguration.class);
      when(globalConfig.transport()).thenReturn(transportConfiguration);
      when(transportConfiguration.distributedSyncTimeout()).thenReturn(30000L);

      Configuration cacheConfig = mock(Configuration.class);
      ClusteringConfiguration clusteringConfiguration = mock(ClusteringConfiguration.class);
      when(cacheConfig.clustering()).thenReturn(clusteringConfiguration);
      when(clusteringConfiguration.cacheMode()).thenReturn(cacheMode);

      return new CapacityFactorRuntimeAttributeValidator(CACHE_NAME, globalConfig, cacheConfig, helper, transport);
   }

   private CapacityFactorUpdateCommand captureCommand() {
      ArgumentCaptor<CapacityFactorUpdateCommand> captor = ArgumentCaptor.forClass(CapacityFactorUpdateCommand.class);
      verify(helper).executeOnCoordinator(any(), captor.capture(), anyLong());
      return captor.getValue();
   }

   private void verifyCommand(float expectedCapacityFactor) {
      CapacityFactorUpdateCommand command = captureCommand();
      assertThat(command.cacheName()).isEqualTo(CACHE_NAME);
      assertThat(command.capacityFactor()).isEqualTo(expectedCapacityFactor);
   }

   public void testLocalCacheSkipsRpc() {
      CapacityFactorRuntimeAttributeValidator validator = createValidator(CacheMode.LOCAL, false);

      validator.validate(0.5f);

      verify(helper, never()).executeOnCoordinator(any(), any(), anyLong());
   }

   public void testDistributedSendsRpc() {
      CapacityFactorRuntimeAttributeValidator validator = createValidator(CacheMode.DIST_SYNC, false);

      validator.validate(0.5f);

      verifyCommand(0.5f);
   }

   public void testDistributedZeroSendsRpc() {
      CapacityFactorRuntimeAttributeValidator validator = createValidator(CacheMode.DIST_SYNC, false);

      validator.validate(0f);

      verifyCommand(0f);
   }

   public void testZeroCapacityNodeRejectsIncrease() {
      CapacityFactorRuntimeAttributeValidator validator = createValidator(CacheMode.DIST_SYNC, true);

      assertThatThrownBy(() -> validator.validate(1.0f))
            .isInstanceOf(IllegalStateException.class);

      verify(helper, never()).executeOnCoordinator(any(), any(), anyLong());
   }

   public void testZeroCapacityNodeAcceptsZero() {
      CapacityFactorRuntimeAttributeValidator validator = createValidator(CacheMode.DIST_SYNC, true);

      validator.validate(0f);

      verifyCommand(0f);
   }

   public void testReplicatedRejectsNonBinary() {
      CapacityFactorRuntimeAttributeValidator validator = createValidator(CacheMode.REPL_SYNC, false);

      assertThatThrownBy(() -> validator.validate(0.5f))
            .isInstanceOf(IllegalArgumentException.class);

      verify(helper, never()).executeOnCoordinator(any(), any(), anyLong());
   }

   public void testReplicatedAcceptsZero() {
      CapacityFactorRuntimeAttributeValidator validator = createValidator(CacheMode.REPL_SYNC, false);

      validator.validate(0f);

      verifyCommand(0f);
   }

   public void testReplicatedAcceptsOne() {
      CapacityFactorRuntimeAttributeValidator validator = createValidator(CacheMode.REPL_SYNC, false);

      validator.validate(1f);

      verifyCommand(1f);
   }

   public void testInvalidationRejectsNonBinary() {
      CapacityFactorRuntimeAttributeValidator validator = createValidator(CacheMode.INVALIDATION_SYNC, false);

      assertThatThrownBy(() -> validator.validate(0.5f))
            .isInstanceOf(IllegalArgumentException.class);

      verify(helper, never()).executeOnCoordinator(any(), any(), anyLong());
   }

   public void testInvalidationAcceptsZero() {
      CapacityFactorRuntimeAttributeValidator validator = createValidator(CacheMode.INVALIDATION_SYNC, false);

      validator.validate(0f);

      verifyCommand(0f);
   }
}
