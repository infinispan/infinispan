package test.infinispan.integration;

import org.springframework.context.annotation.Bean;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.session.web.http.HeaderHttpSessionIdResolver;
import org.springframework.session.web.http.HttpSessionIdResolver;

public class SecurityConfig {

   @Bean
   public InMemoryUserDetailsManager userDetailsService() {
      UserDetails user = User.withDefaultPasswordEncoder()
            .username("user")
            .password("password")
            .roles("USER")
            .build();
      return new InMemoryUserDetailsManager(user);
   }

   @Bean
   public SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
      http
            .authorizeHttpRequests((requests) -> requests
                  .anyRequest().authenticated()
            )
            .sessionManagement(sm -> sm.sessionCreationPolicy(SessionCreationPolicy.IF_REQUIRED))
            .formLogin(Customizer.withDefaults())
            .httpBasic(Customizer.withDefaults());

      return http.build();
   }

   @Bean
   public HttpSessionIdResolver httpSessionStrategy() {
      return new HeaderHttpSessionIdResolver("x-auth-token");
   }

   @Bean
   public InfinispanSessionListener httpSessionListener(){
      return new InfinispanSessionListener();
   }
}
