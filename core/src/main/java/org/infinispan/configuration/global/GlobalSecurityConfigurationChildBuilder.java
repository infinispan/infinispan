package org.infinispan.configuration.global;

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
    * Defines the timeout in milliseconds for which to cache user access roles
    *
    * @param securityCacheTimeout
    */
   GlobalSecurityConfigurationBuilder securityCacheTimeout(long securityCacheTimeout);
}
