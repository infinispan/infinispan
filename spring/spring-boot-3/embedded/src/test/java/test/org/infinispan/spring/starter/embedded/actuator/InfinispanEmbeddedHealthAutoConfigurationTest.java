package test.org.infinispan.spring.starter.embedded.actuator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.spring.starter.embedded.InfinispanEmbeddedHealthAutoConfiguration.HEALTH_INDICATOR_NAME;

import java.util.List;
import java.util.Map;

import org.infinispan.commons.util.Version;
import org.infinispan.health.HealthStatus;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.remoting.transport.NodeVersion;
import org.infinispan.spring.starter.embedded.InfinispanEmbeddedAutoConfiguration;
import org.infinispan.spring.starter.embedded.InfinispanEmbeddedCacheManagerAutoConfiguration;
import org.infinispan.spring.starter.embedded.InfinispanEmbeddedHealthAutoConfiguration;
import org.infinispan.spring.starter.embedded.InfinispanGlobalConfigurationCustomizer;
import org.infinispan.spring.starter.embedded.actuator.InfinispanEmbeddedHealthIndicator;
import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

public class InfinispanEmbeddedHealthAutoConfigurationTest {

   private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
         // Other tests in this module keep their application context open, so the default JMX domain stays taken.
         .withUserConfiguration(JmxDisabledConfiguration.class)
         .withConfiguration(AutoConfigurations.of(
               InfinispanEmbeddedAutoConfiguration.class,
               InfinispanEmbeddedCacheManagerAutoConfiguration.class,
               InfinispanEmbeddedHealthAutoConfiguration.class));

   @Test
   public void testHealthIndicatorIsRegisteredByDefault() {
      contextRunner.run(context -> assertThat(context)
            .hasBean(HEALTH_INDICATOR_NAME)
            .hasSingleBean(InfinispanEmbeddedHealthIndicator.class));
   }

   @Test
   public void testLocalCacheManagerIsUp() {
      contextRunner.run(context -> {
         Health health = context.getBean(InfinispanEmbeddedHealthIndicator.class).health();

         assertThat(health.getStatus()).isEqualTo(Status.UP);
         assertThat(health.getDetails())
               .containsEntry("cluster_health", HealthStatus.HEALTHY.name())
               .containsEntry("cluster_name", "default-autoconfigure")
               .containsEntry("cluster_size", 1)
               .containsEntry("node_address", "local")
               .containsEntry("cache_manager_status", ComponentStatus.RUNNING.toString())
               .containsEntry("version", Version.getVersion())
               .containsEntry("rolling_upgrade", false);
         assertThat(members(health)).containsExactly(
               Map.of("node_address", "local", "version", NodeVersion.INSTANCE.toString()));
      });
   }

   @Test
   public void testMembershipCanBeDisabled() {
      contextRunner.withPropertyValues("infinispan.embedded.health.membership.enabled=false").run(context -> {
         Health health = context.getBean(InfinispanEmbeddedHealthIndicator.class).health();

         assertThat(health.getStatus()).isEqualTo(Status.UP);
         assertThat(health.getDetails()).doesNotContainKeys("members", "rolling_upgrade");
      });
   }

   @Test
   public void testRebalancingStatusIsConfigurable() {
      contextRunner.withPropertyValues("infinispan.embedded.health.rebalancing-status=down")
            .run(context -> assertThat(context).hasSingleBean(InfinispanEmbeddedHealthIndicator.class));
   }

   @Test
   public void testHealthIndicatorCanBeDisabled() {
      contextRunner.withPropertyValues("infinispan.embedded.health.enabled=false")
            .run(context -> assertThat(context).doesNotHaveBean(InfinispanEmbeddedHealthIndicator.class));
   }

   @Test
   public void testHealthIndicatorIsAbsentWhenTheStarterIsDisabled() {
      contextRunner.withPropertyValues("infinispan.embedded.enabled=false")
            .run(context -> assertThat(context).doesNotHaveBean(InfinispanEmbeddedHealthIndicator.class));
   }

   @Test
   public void testHealthIndicatorIsAbsentWithoutACacheManager() {
      contextRunner.withPropertyValues("spring.cache.type=NONE").run(context -> {
         assertThat(context).hasNotFailed();
         assertThat(context).doesNotHaveBean(InfinispanEmbeddedHealthIndicator.class);
      });
   }

   @Test
   public void testHealthIndicatorIsAbsentWithoutActuator() {
      contextRunner.withClassLoader(new FilteredClassLoader(HealthIndicator.class)).run(context -> {
         assertThat(context).hasNotFailed();
         assertThat(context).doesNotHaveBean(HEALTH_INDICATOR_NAME);
      });
   }

   @Test
   public void testUserDefinedIndicatorWins() {
      contextRunner.withUserConfiguration(CustomHealthIndicatorConfiguration.class).run(context -> {
         assertThat(context).doesNotHaveBean(InfinispanEmbeddedHealthIndicator.class);
         assertThat(context.getBean(HEALTH_INDICATOR_NAME, HealthIndicator.class).health().getDetails())
               .containsEntry("custom", true);
      });
   }

   @SuppressWarnings("unchecked")
   private static List<Map<String, Object>> members(Health health) {
      return (List<Map<String, Object>>) health.getDetails().get("members");
   }

   @Configuration(proxyBeanMethods = false)
   static class JmxDisabledConfiguration {

      @Bean
      public InfinispanGlobalConfigurationCustomizer jmxDisabledCustomizer() {
         return builder -> builder.jmx().disable();
      }
   }

   @Configuration(proxyBeanMethods = false)
   static class CustomHealthIndicatorConfiguration {

      @Bean(HEALTH_INDICATOR_NAME)
      public HealthIndicator customHealthIndicator() {
         return () -> Health.up().withDetail("custom", true).build();
      }
   }
}
