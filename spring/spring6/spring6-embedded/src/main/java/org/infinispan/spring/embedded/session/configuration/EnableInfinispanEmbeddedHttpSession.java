package org.infinispan.spring.embedded.session.configuration;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.session.MapSession;
import org.springframework.session.config.annotation.web.http.EnableSpringHttpSession;

/**
 * Add this annotation to a {@code @Configuration} class to expose the SessionRepositoryFilter as a bean named
 * "springSessionRepositoryFilter" and backed on Infinispan.
 * <p>
 * The configuration requires creating a {@link org.infinispan.spring.common.provider.SpringCache} (for either remote or
 * embedded configuration). Here's an example:
 * <pre> <code>
 * {@literal @Configuration}
 * {@literal @EnableInfinispanEmbeddednHttpSession}
 * public class InfinispanConfiguration {
 *
 *     {@literal @Bean}
 *     public SpringEmbeddedCacheManagerFactoryBean springCache() {
 *         return new SpringEmbeddedCacheManagerFactoryBean();
 *     }
 * }
 * </code> </pre>
 *
 * Configuring advanced features requires putting everything together manually or extending
 * {@link InfinispanEmbeddedHttpSessionConfiguration}.
 *
 * @author Sebastian Łaskawiec
 * @see EnableSpringHttpSession
 * @since 9.0
 */
@Retention(java.lang.annotation.RetentionPolicy.RUNTIME)
@Target({java.lang.annotation.ElementType.TYPE})
@Documented
@Import(InfinispanEmbeddedHttpSessionConfiguration.class)
@Configuration
public @interface EnableInfinispanEmbeddedHttpSession {

   String DEFAULT_CACHE_NAME = "sessions";

   /**
    * This is the session timeout in seconds. By default, it is set to 1800 seconds (30 minutes). This should be a
    * non-negative integer.
    *
    * @return the seconds a session can be inactive before expiring
    */
   int maxInactiveIntervalInSeconds() default MapSession.DEFAULT_MAX_INACTIVE_INTERVAL_SECONDS;

   /**
    * Cache name used for storing session data.
    *
    * @return the cache name for storing data.
    */
   String cacheName() default DEFAULT_CACHE_NAME;

}
