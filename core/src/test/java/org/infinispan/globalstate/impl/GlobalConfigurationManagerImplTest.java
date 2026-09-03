package org.infinispan.globalstate.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.infinispan.commons.configuration.io.ConfigurationSchemaVersion;
import org.infinispan.remoting.transport.NodeVersion;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "globalstate.impl.GlobalConfigurationManagerImplTest")
public class GlobalConfigurationManagerImplTest extends AbstractInfinispanTest {

   public void testTargetSchemaVersionDerivedFromOldestClusterMember() {
      GlobalConfigurationManagerImpl manager = new GlobalConfigurationManagerImpl();
      Transport transport = mock(Transport.class);
      when(transport.getOldestMember()).thenReturn(NodeVersion.from((byte) 16, (byte) 2, (byte) 0));
      manager.transport = transport;

      ConfigurationSchemaVersion target = manager.targetSchemaVersion();

      assertThat(target).isNotNull();
      assertThat(target.getMajor()).isEqualTo(16);
      assertThat(target.getMinor()).isEqualTo(2);
   }

   public void testTargetSchemaVersionNullWithoutTransport() {
      GlobalConfigurationManagerImpl manager = new GlobalConfigurationManagerImpl();

      assertThat(manager.targetSchemaVersion()).isNull();
   }
}
