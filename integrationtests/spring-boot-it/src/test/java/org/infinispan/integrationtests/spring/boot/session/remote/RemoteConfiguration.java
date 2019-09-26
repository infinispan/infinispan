package org.infinispan.integrationtests.spring.boot.session.remote;

import java.net.InetSocketAddress;
import java.util.Arrays;

import org.infinispan.integrationtests.spring.boot.session.configuration.WebConfig;
import org.infinispan.spring.remote.AbstractRemoteCacheManagerFactory;
import org.infinispan.spring.remote.provider.SpringRemoteCacheManagerFactoryBean;
import org.infinispan.spring.remote.session.configuration.EnableInfinispanRemoteHttpSession;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.util.SocketUtils;

@Configuration
@EnableAutoConfiguration
@EnableInfinispanRemoteHttpSession
@Import(WebConfig.class)
public class RemoteConfiguration {

   static final int SERVER_PORT = SocketUtils.findAvailableTcpPort();

   @Bean
   public SpringRemoteCacheManagerFactoryBean springCacheManager() {
      SpringRemoteCacheManagerFactoryBean factoryBean = new SpringRemoteCacheManagerFactoryBean();
      factoryBean.setServerList(Arrays.asList(new InetSocketAddress("localhost", SERVER_PORT)));
      factoryBean.setClassWhiteList(AbstractRemoteCacheManagerFactory.SPRING_JAVA_SERIAL_WHITELIST + ",java.util.*");
      return factoryBean;
   }
}
