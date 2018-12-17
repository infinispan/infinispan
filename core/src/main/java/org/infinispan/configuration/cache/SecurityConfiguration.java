package org.infinispan.configuration.cache;

import static org.infinispan.configuration.parsing.Element.SECURITY;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.Matchable;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;

/**
 * SecurityConfiguration.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class SecurityConfiguration implements Matchable<SecurityConfiguration>, ConfigurationInfo {

   private final AuthorizationConfiguration authorization;

   private final List<ConfigurationInfo> subElement = new ArrayList<>();

   public static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(SECURITY.getLocalName());

   SecurityConfiguration(AuthorizationConfiguration authorization) {
      this.authorization = authorization;
      subElement.add(authorization);
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   public AuthorizationConfiguration authorization() {
      return authorization;
   }

   @Override
   public String toString() {
      return "SecurityConfiguration [authorization=" + authorization + "]";
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((authorization == null) ? 0 : authorization.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      SecurityConfiguration other = (SecurityConfiguration) obj;
      if (authorization == null) {
         if (other.authorization != null)
            return false;
      } else if (!authorization.equals(other.authorization))
         return false;
      return true;
   }

   @Override
   public AttributeSet attributes() {
      return null;
   }

   @Override
   public List<ConfigurationInfo> subElements() {
      return subElement;
   }
}
