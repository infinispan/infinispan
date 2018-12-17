package org.infinispan.persistence.remote.configuration;

import java.util.Arrays;
import java.util.Collection;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.ElementDefinition;

/**
 * SecurityConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 9.1
 */
public class SecurityConfigurationBuilder extends AbstractRemoteStoreConfigurationChildBuilder implements
      Builder<SecurityConfiguration>, ConfigurationBuilderInfo {

   private final AuthenticationConfigurationBuilder authentication = new AuthenticationConfigurationBuilder(this);
   private final SslConfigurationBuilder ssl = new SslConfigurationBuilder(this);

   SecurityConfigurationBuilder(RemoteStoreConfigurationBuilder builder) {
      super(builder, null);
   }

   public AuthenticationConfigurationBuilder authentication() {
      return authentication;
   }

   public SslConfigurationBuilder ssl() {
      return ssl;
   }

   @Override
   public SecurityConfiguration create() {
      return new SecurityConfiguration(authentication.create(), ssl.create());
   }

   @Override
   public Builder<?> read(SecurityConfiguration template) {
      authentication.read(template.authentication());
      ssl.read(template.ssl());
      return this;
   }

   @Override
   public Collection<ConfigurationBuilderInfo> getChildrenInfo() {
      return Arrays.asList(authentication, ssl);
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return SecurityConfiguration.ELEMENT_DEFINITION;
   }

   @Override
   public void validate() {
      authentication.validate();
      ssl.validate();
   }

   @Override
   public String toString() {
      return "SecurityConfigurationBuilder{" +
            "authentication=" + authentication +
            ", ssl=" + ssl +
            '}';
   }
}
