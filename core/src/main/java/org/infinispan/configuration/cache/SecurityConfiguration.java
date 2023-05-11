package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.configuration.parsing.Element;

/**
 * SecurityConfiguration.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class SecurityConfiguration extends ConfigurationElement<SecurityConfiguration> {

   private final AuthorizationConfiguration authorization;

   SecurityConfiguration(AuthorizationConfiguration authorization) {
      super(Element.SECURITY, AttributeSet.EMPTY, authorization);
      this.authorization = authorization;
   }

   public AuthorizationConfiguration authorization() {
      return authorization;
   }
}
