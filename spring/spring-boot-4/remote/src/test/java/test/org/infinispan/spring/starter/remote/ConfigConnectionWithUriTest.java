package test.org.infinispan.spring.starter.remote;

import static org.assertj.core.api.Assertions.assertThat;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.spring.starter.remote.InfinispanRemoteAutoConfiguration;
import org.infinispan.spring.starter.remote.InfinispanRemoteCacheManagerAutoConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest(
      classes = {
            InfinispanRemoteAutoConfiguration.class,
            InfinispanRemoteCacheManagerAutoConfiguration.class
      },
      properties = {
            "spring.main.banner-mode=off"
      })
@TestPropertySource(locations = "classpath:config-uri-test-hotrod-client.properties")
public class ConfigConnectionWithUriTest {

   @Autowired
   private RemoteCacheManager remoteCacheManager;


   @Test
   public void testClientIsCreated() {
      assertThat(remoteCacheManager).isNotNull();
   }

}
