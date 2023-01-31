package org.infinispan.server.core.configuration;

import org.infinispan.commons.configuration.Builder;

/**
 * @since 15.0
 **/
public interface AuthenticationConfigurationBuilder<A extends AuthenticationConfiguration> extends Builder<A> {

   AuthenticationConfigurationBuilder<A> enable();

   String securityRealm();
}
