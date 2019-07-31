package org.infinispan.persistence.remote.configuration;

import static org.infinispan.persistence.remote.configuration.Element.SECURITY;

import java.util.Arrays;
import java.util.List;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;

/**
 * SecurityConfiguration.
 *
 * @author Tristan Tarrant
 * @since 9.1
 */
public class SecurityConfiguration implements ConfigurationInfo {

   private final AuthenticationConfiguration authentication;
   private final SslConfiguration ssl;

   static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(SECURITY.getLocalName());

   SecurityConfiguration(AuthenticationConfiguration authentication, SslConfiguration ssl) {
      this.authentication = authentication;
      this.ssl = ssl;
   }

   public AuthenticationConfiguration authentication() {
      return authentication;
   }

   public SslConfiguration ssl() {
      return ssl;
   }

   @Override
   public List<ConfigurationInfo> subElements() {
      return Arrays.asList(authentication, ssl);
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }


   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      SecurityConfiguration that = (SecurityConfiguration) o;

      if (!authentication.equals(that.authentication)) return false;
      return ssl.equals(that.ssl);
   }

   @Override
   public int hashCode() {
      int result = authentication.hashCode();
      result = 31 * result + ssl.hashCode();
      return result;
   }

   @Override
   public String toString() {
      return "SecurityConfiguration{" +
            "authentication=" + authentication +
            ", ssl=" + ssl +
            '}';
   }
}
