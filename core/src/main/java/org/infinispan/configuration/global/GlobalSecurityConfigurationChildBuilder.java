package org.infinispan.configuration.global;

import java.util.concurrent.TimeUnit;

/**
 * GlobalSecurityConfigurationChildBuilder.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public interface GlobalSecurityConfigurationChildBuilder extends GlobalConfigurationChildBuilder {
   /**
    * Defines global roles as groups of permissions
    */
   GlobalAuthorizationConfigurationBuilder authorization();
   /**
    * Defines the timeout in milliseconds for which to cache user access roles. A value of -1 means the entries
    * never expire. A value of 0 will disable the cache.
    *
    * @param securityCacheTimeout
    * @deprecated Use {@link #securityCacheTimeout(long, TimeUnit)} instead
    */
   @Deprecated
   GlobalSecurityConfigurationBuilder securityCacheTimeout(long securityCacheTimeout);

   /**
    * Defines the timeout for which to cache user access roles. A value of -1 means the entries
    * never expire. A value of 0 will disable the cache.
    *
    * @param securityCacheTimeout the timeout
    * @param unit the {@link TimeUnit}
    */
   GlobalSecurityConfigurationBuilder securityCacheTimeout(long securityCacheTimeout, TimeUnit unit);
}
