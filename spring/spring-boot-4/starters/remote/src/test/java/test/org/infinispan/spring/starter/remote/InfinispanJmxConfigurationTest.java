package test.org.infinispan.spring.starter.remote;

import static org.assertj.core.api.Assertions.assertThat;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.spring.starter.remote.InfinispanJmxConfiguration;
import org.infinispan.spring.starter.remote.InfinispanRemoteAutoConfiguration;
import org.infinispan.spring.starter.remote.InfinispanRemoteCacheManagerAutoConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jmx.export.MBeanExporter;

@SpringBootTest(classes = {
      InfinispanRemoteAutoConfiguration.class,
      InfinispanRemoteCacheManagerAutoConfiguration.class,
      InfinispanJmxConfiguration.class,
      MBeanExporter.class},
      properties = {
            "spring.main.banner-mode=off",
            "infinispan.remote.server-list=180.567.112.333:6668",
            "infinispan.remote.jmx=true",
            "infinispan.remote.enabled=true"})
public class InfinispanJmxConfigurationTest {

   @Autowired
   RemoteCacheManager remoteCacheManager;

   @Test
   public void contextIsUp() {
      /**
       *  When JMX is enabled in Spring and Infinispan, if {@link InfinispanJmxConfiguration} does not exclude
       *  'remoteCacheManager', this test will fail because context won't start
       */
      assertThat(remoteCacheManager).isNotNull();
   }
}
