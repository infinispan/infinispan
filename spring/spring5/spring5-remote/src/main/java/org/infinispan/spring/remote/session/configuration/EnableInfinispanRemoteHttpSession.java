package org.infinispan.spring.remote.session.configuration;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.task.TaskExecutor;
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
 * {@literal @EnableInfinispanRemoteHttpSession}
 * public class InfinispanConfiguration {
 *
 *     {@literal @Bean}
 *     public SpringRemoteCacheManagerFactoryBean springCache() {
 *         return new SpringRemoteCacheManagerFactoryBean();
 *     }
 * }
 * </code> </pre>
 *
 * Configuring advanced features requires putting everything together manually or extending
 * {@link InfinispanRemoteHttpSessionConfiguration}.
 *
 * @author Sebastian Łaskawiec
 * @see EnableSpringHttpSession
 * @since 9.0
 */
@Retention(java.lang.annotation.RetentionPolicy.RUNTIME)
@Target({java.lang.annotation.ElementType.TYPE})
@Documented
@Import(InfinispanRemoteHttpSessionConfiguration.class)
@Configuration
public @interface EnableInfinispanRemoteHttpSession {

   String DEFAULT_CACHE_NAME = "sessions";
   String DEFAULT_EXECUTOR_THREAD_NAME_PREFIX = "infinispan_remote_task_executor_thread";
   int DEFAULT_EXECUTOR_POOL_SIZE = 4;
   int DEFAULT_EXECUTOR_MAX_POOL_SIZE = 4;

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

   /**
    * Used to configure a {@link TaskExecutor} used by the the Remote client to be able to execute calls to the cache from the listeners
    *
    * @return the pool size
    */
   int executorPoolSize() default DEFAULT_EXECUTOR_POOL_SIZE;

   /**
    * Used to configure a {@link TaskExecutor} used by the the Remote client to be able to execute calls to the cache from the listeners
    *
    * @return the max pool size
    */
   int executorMaxPoolSize() default DEFAULT_EXECUTOR_MAX_POOL_SIZE;

   /**
    * Used to configure a {@link TaskExecutor} used by the the Remote client to be able to execute calls to the cache from the listeners
    *
    * @return the thread name prefix
    */
   String executorThreadNamePrefix() default DEFAULT_EXECUTOR_THREAD_NAME_PREFIX;

}
