package test.infinispan.integration.remote;

import org.infinispan.spring.remote.AbstractRemoteCacheManagerFactory;
import org.infinispan.spring.remote.provider.SpringRemoteCacheManagerFactoryBean;
import org.infinispan.spring.remote.session.configuration.EnableInfinispanRemoteHttpSession;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.test.util.TestSocketUtils;
import test.infinispan.integration.SecurityConfig;

import java.net.InetSocketAddress;
import java.util.Arrays;

@SpringBootApplication
@EnableInfinispanRemoteHttpSession
@EnableCaching
@EnableWebSecurity
@Import(SecurityConfig.class)
public class RemoteSessionApp {

   static final int SERVER_PORT = TestSocketUtils.findAvailableTcpPort();

   @Bean
   @Primary
   public SpringRemoteCacheManagerFactoryBean springCacheManager() {
      SpringRemoteCacheManagerFactoryBean factoryBean = new SpringRemoteCacheManagerFactoryBean();
      factoryBean.setServerList(Arrays.asList(new InetSocketAddress("localhost", SERVER_PORT)));
      factoryBean.setClassAllowList(AbstractRemoteCacheManagerFactory.SPRING_JAVA_SERIAL_ALLOWLIST + ",java.util.*");
      return factoryBean;
   }
}
