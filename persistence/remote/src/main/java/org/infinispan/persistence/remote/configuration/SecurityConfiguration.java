package org.infinispan.persistence.remote.configuration;

import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;

/**
 * SecurityConfiguration.
 *
 * @author Tristan Tarrant
 * @since 9.1
 */
public class SecurityConfiguration extends ConfigurationElement<SecurityConfiguration> {

   private final AuthenticationConfiguration authentication;
   private final SslConfiguration ssl;

   SecurityConfiguration(AuthenticationConfiguration authentication, SslConfiguration ssl) {
      super(Element.SECURITY, AttributeSet.EMPTY, authentication, ssl);
      this.authentication = authentication;
      this.ssl = ssl;
   }

   public AuthenticationConfiguration authentication() {
      return authentication;
   }

   public SslConfiguration ssl() {
      return ssl;
   }
}
