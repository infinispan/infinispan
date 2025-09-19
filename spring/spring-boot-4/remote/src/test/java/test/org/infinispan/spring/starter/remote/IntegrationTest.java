package test.org.infinispan.spring.starter.remote;

import static org.assertj.core.api.Assertions.assertThat;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.spring.remote.provider.SpringRemoteCacheManager;
import org.infinispan.spring.starter.remote.InfinispanRemoteAutoConfiguration;
import org.infinispan.spring.starter.remote.InfinispanRemoteCacheManagerAutoConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import test.org.infinispan.spring.starter.remote.testconfiguration.InfinispanCacheTestConfiguration;

@SpringBootTest(
      classes = {
            InfinispanRemoteAutoConfiguration.class,
            InfinispanRemoteCacheManagerAutoConfiguration.class,
            InfinispanCacheTestConfiguration.class
      },
      properties = {
            "spring.main.banner-mode=off",
            "infinispan.remote.client-properties=test-hotrod-client.properties"
      }
)
public class IntegrationTest {

   @Autowired
   private RemoteCacheManager remoteCacheManager;

   @Autowired
   private SpringRemoteCacheManager springRemoteCacheManager;

   @Test
   public void testConfiguredClient() {
      int portObtainedFromPropertiesFile = remoteCacheManager.getConfiguration().servers().get(0).port();

      assertThat(portObtainedFromPropertiesFile).isEqualTo(11555);
   }

   @Test
   public void testIfSpringCachingWasCreatedUsingProperEmbeddedCacheManager() throws Exception {
      assertThat(remoteCacheManager).isEqualTo(springRemoteCacheManager.getNativeCacheManager());
   }
}
