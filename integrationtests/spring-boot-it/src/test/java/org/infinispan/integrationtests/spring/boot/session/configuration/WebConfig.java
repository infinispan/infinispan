package org.infinispan.integrationtests.spring.boot.session.configuration;

import org.infinispan.integrationtests.spring.boot.session.web.TestRESTController;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.crypto.password.NoOpPasswordEncoder;
import org.springframework.session.web.http.HeaderHttpSessionIdResolver;
import org.springframework.session.web.http.HttpSessionIdResolver;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

@Configuration
@EnableWebMvc
public class WebConfig {

   @Bean
   public HttpSessionIdResolver httpSessionStrategy() {
      return new HeaderHttpSessionIdResolver("x-auth-token");
   }

   @Bean
   public TestRESTController sessionCreator() {
      return new TestRESTController();
   }

   @Bean
   public SecurityConfig securityConfig() {
      return new SecurityConfig();
   }

   @Bean
   public InfinispanSessionListener httpSessionListener(){
      return new InfinispanSessionListener();
   }

   @Bean
   public NoOpPasswordEncoder passwordEncoder() {
      return (NoOpPasswordEncoder) NoOpPasswordEncoder.getInstance();
   }
}
