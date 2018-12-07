package org.infinispan.integrationtests.spring.boot.session.remote;

import java.net.InetSocketAddress;
import java.util.Arrays;

import org.infinispan.integrationtests.spring.boot.session.configuration.WebConfig;
import org.infinispan.spring.remote.provider.SpringRemoteCacheManagerFactoryBean;
import org.infinispan.spring.remote.session.configuration.EnableInfinispanRemoteHttpSession;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.util.SocketUtils;

@Configuration
@EnableAutoConfiguration
@EnableInfinispanRemoteHttpSession(executorPoolSize = 1, executorMaxPoolSize = 1)
@Import(WebConfig.class)
public class RemoteConfiguration {

   public static final int SERVER_PORT = SocketUtils.findAvailableTcpPort();

   @Bean
   public SpringRemoteCacheManagerFactoryBean springCacheManager() {
      SpringRemoteCacheManagerFactoryBean factoryBean = new SpringRemoteCacheManagerFactoryBean();
      factoryBean.setServerList(Arrays.asList(new InetSocketAddress("localhost", SERVER_PORT)));
      return factoryBean;
   }

}
