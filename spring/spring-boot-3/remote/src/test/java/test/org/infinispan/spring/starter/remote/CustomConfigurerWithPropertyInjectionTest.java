package test.org.infinispan.spring.starter.remote;

import static org.assertj.core.api.Assertions.assertThat;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.spring.starter.remote.InfinispanRemoteAutoConfiguration;
import org.infinispan.spring.starter.remote.InfinispanRemoteConfigurer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@SpringBootTest(
      classes = {
            CustomConfigurerWithPropertyInjectionTest.TestConfiguration.class,
            InfinispanRemoteAutoConfiguration.class
      },
      properties = {
            "spring.main.banner-mode=off",
            "myServerList=localhost:6667"
      }
)
public class CustomConfigurerWithPropertyInjectionTest {
   @Autowired
   private RemoteCacheManager manager;

   @Test
   public void testConfiguredClient() {
      assertThat(manager.getConfiguration().servers().get(0).port()).isEqualTo(6667);
   }

   @Configuration
   static class TestConfiguration {
      @Value("${myServerList}")
      private String serverList;

      @Bean
      public InfinispanRemoteConfigurer configuration() {
         return () -> new ConfigurationBuilder()
               .addServers(serverList)
               .build();
      }
   }
}
