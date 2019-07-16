package org.infinispan.configuration.global;

import java.util.Collections;
import java.util.List;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.configuration.parsing.Element;

/**
 * GlobalSecurityConfiguration.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class GlobalSecurityConfiguration implements ConfigurationInfo {
   private final GlobalAuthorizationConfiguration authorization;
   private final long securityCacheTimeout;

   private static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(Element.SECURITY.getLocalName());
   private final List<ConfigurationInfo> subElements;

   GlobalSecurityConfiguration(GlobalAuthorizationConfiguration authorization, long securityCacheTimeout) {
      this.authorization = authorization;
      this.securityCacheTimeout = securityCacheTimeout;
      this.subElements = Collections.singletonList(authorization);
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   @Override
   public List<ConfigurationInfo> subElements() {
      return subElements;
   }

   public GlobalAuthorizationConfiguration authorization() {
      return authorization;
   }

   public long securityCacheTimeout() {
      return securityCacheTimeout;
   }

   @Override
   public String toString() {
      return "GlobalSecurityConfiguration [authorization=" + authorization
            + ", securityCacheTimeout=" + securityCacheTimeout + "]";
   }

}
