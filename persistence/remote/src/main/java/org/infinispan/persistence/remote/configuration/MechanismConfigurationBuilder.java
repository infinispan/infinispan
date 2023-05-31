package org.infinispan.persistence.remote.configuration;

import static org.infinispan.persistence.remote.configuration.MechanismConfiguration.PASSWORD;
import static org.infinispan.persistence.remote.configuration.MechanismConfiguration.SASL_MECHANISM;
import static org.infinispan.persistence.remote.configuration.MechanismConfiguration.USERNAME;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * @since 10.0
 */
public class MechanismConfigurationBuilder extends AbstractSecurityConfigurationChildBuilder implements Builder<MechanismConfiguration> {

   MechanismConfigurationBuilder(SecurityConfigurationBuilder builder) {
      super(builder, MechanismConfiguration.attributeDefinitionSet());
   }

   public AttributeSet attributes() {
      return attributes;
   }

   public MechanismConfigurationBuilder saslMechanism(String mechanism) {
      this.attributes.attribute(SASL_MECHANISM).set(mechanism);
      return this;
   }

   public MechanismConfigurationBuilder username(String username) {
      this.attributes.attribute(USERNAME).set(username);
      return this;
   }

   public MechanismConfigurationBuilder password(String password) {
      this.attributes.attribute(PASSWORD).set(password);
      return this;
   }

   public MechanismConfigurationBuilder password(char[] password) {
      this.password(new String(password));
      return this;
   }

   public MechanismConfigurationBuilder realm(String realm) {
      this.attributes.attribute(MechanismConfiguration.REALM).set(realm);
      return this;
   }

   @Override
   public MechanismConfiguration create() {
      return new MechanismConfiguration(attributes.protect());
   }

   @Override
   public Builder<?> read(MechanismConfiguration template, Combine combine) {
      this.attributes.read(template.attributes(), combine);
      return this;
   }
}
