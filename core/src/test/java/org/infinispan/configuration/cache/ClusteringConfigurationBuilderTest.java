package org.infinispan.configuration.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.util.TimeQuantity;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "configuration.cache.ClusteringConfigurationBuilderTest")
public class ClusteringConfigurationBuilderTest {
   @DataProvider
   public Object[][] invalidRemoteTimeouts() {
      return new Object[][] {{0L}, {-1L}, {Long.MIN_VALUE}};
   }

   @Test(dataProvider = "invalidRemoteTimeouts")
   public void testRejectNonpositiveRemoteTimeout(long timeout) {
      for (CacheMode mode : CacheMode.values()) {
         ConfigurationBuilder builder = new ConfigurationBuilder();
         builder.clustering().cacheMode(mode).remoteTimeout(timeout).stateTransfer().timeout(-1);
         builder.locking().lockAcquisitionTimeout(-1);
         assertThatThrownBy(builder::build)
               .isInstanceOf(CacheConfigurationException.class)
               .hasMessageContaining("ISPN000951");
         assertThatThrownBy(() -> builder.validate(GlobalConfigurationBuilder.defaultClusteredBuilder().build()))
               .isInstanceOf(CacheConfigurationException.class)
               .hasMessageContaining("ISPN000951");
      }
   }

   @Test(dataProvider = "invalidRemoteTimeouts")
   public void testRejectInvalidRuntimeUpdateWithoutChangingValue(long timeout) {
      ClusteringConfiguration clustering = new ConfigurationBuilder().build().clustering();
      AtomicInteger notifications = new AtomicInteger();
      clustering.attributes().attribute(ClusteringConfiguration.REMOTE_TIMEOUT)
            .addListener((attribute, previous) -> notifications.incrementAndGet());

      assertThatThrownBy(() -> clustering.remoteTimeout(timeout)).isInstanceOf(CacheConfigurationException.class);
      assertThatThrownBy(() -> clustering.remoteTimeout(timeout + "ms")).isInstanceOf(CacheConfigurationException.class);
      assertThatThrownBy(() -> clustering.attributes().attribute(ClusteringConfiguration.REMOTE_TIMEOUT)
            .fromString(Long.toString(timeout))).isInstanceOf(CacheConfigurationException.class);

      assertThat(clustering.remoteTimeout()).isEqualTo(15000);
      assertThat(notifications).hasValue(0);
   }

   @Test(dataProvider = "invalidRemoteTimeouts")
   public void testRejectInvalidConfigurationUpdate(long timeout) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC).remoteTimeout(timeout);
      ClusteringConfiguration invalid = builder.build(false).clustering();
      ClusteringConfiguration clustering = new ConfigurationBuilder().clustering().cacheMode(CacheMode.DIST_SYNC)
            .build().clustering();

      assertThatThrownBy(() -> clustering.validateUpdate("", invalid)).isInstanceOf(CacheConfigurationException.class);
      assertThatThrownBy(() -> clustering.update("", invalid)).isInstanceOf(CacheConfigurationException.class);
      assertThat(clustering.remoteTimeout()).isEqualTo(15000);
   }

   public void testRejectTimeoutRoundedToZero() {
      assertThatThrownBy(() -> new ConfigurationBuilder().clustering().remoteTimeout(1, TimeUnit.MICROSECONDS).build())
            .isInstanceOf(CacheConfigurationException.class);
      assertThatThrownBy(() -> new ConfigurationBuilder().clustering().remoteTimeout("0.1ms").build())
            .isInstanceOf(CacheConfigurationException.class);
      ClusteringConfiguration clustering = new ConfigurationBuilder().build().clustering();
      assertThatThrownBy(() -> clustering.remoteTimeout("0.1ms")).isInstanceOf(CacheConfigurationException.class);
      assertThat(clustering.remoteTimeout()).isEqualTo(15000);
   }

   public void testPositiveRemoteTimeout() {
      for (long timeout : new long[] {1, 15000, (long) Integer.MAX_VALUE + 1}) {
         ClusteringConfiguration clustering = new ConfigurationBuilder().clustering().remoteTimeout(timeout)
               .build().clustering();
         assertThat(clustering.remoteTimeout()).isEqualTo(timeout);
         clustering.remoteTimeout("1ms");
         assertThat(clustering.remoteTimeout()).isEqualTo(1);
         clustering.remoteTimeout(timeout);
         assertThat(clustering.remoteTimeout()).isEqualTo(timeout);
         clustering.attributes().attribute(ClusteringConfiguration.REMOTE_TIMEOUT).fromString("30s");
         assertThat(clustering.remoteTimeout()).isEqualTo(30000);
      }
   }

   public void testValidateRawAttribute() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().attributes().attribute(ClusteringConfiguration.REMOTE_TIMEOUT).set(TimeQuantity.valueOf(0));
      assertThatThrownBy(builder::build).isInstanceOf(CacheConfigurationException.class);
   }

   @Test(dataProvider = "invalidRemoteTimeouts")
   public void testRejectInvalidParsedConfiguration(long timeout) {
      String xml = """
            <infinispan xmlns="urn:infinispan:config:16.4">
               <cache-container>
                  <distributed-cache name="test" remote-timeout="%s">
                     <state-transfer timeout="-1"/>
                     <locking acquire-timeout="-1"/>
                  </distributed-cache>
               </cache-container>
            </infinispan>
            """.formatted(timeout);
      assertThatThrownBy(() -> new ParserRegistry().parse(xml).getNamedConfigurationBuilders().get("test").build())
            .isInstanceOf(CacheConfigurationException.class)
            .hasMessageContaining("ISPN000951");
   }
}
