package org.infinispan.persistence.remote.configuration;

import static org.infinispan.persistence.remote.configuration.MechanismConfiguration.PASSWORD;
import static org.infinispan.persistence.remote.configuration.MechanismConfiguration.SASL_MECHANISM;
import static org.infinispan.persistence.remote.configuration.MechanismConfiguration.USERNAME;
import static org.infinispan.persistence.remote.configuration.MechanismConfiguration.serializeMechanism;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;

/**
 * @since 10.0
 */
public class MechanismConfigurationBuilder extends AbstractSecurityConfigurationChildBuilder implements Builder<MechanismConfiguration>, ConfigurationBuilderInfo {

   MechanismConfigurationBuilder(SecurityConfigurationBuilder builder) {
      super(builder, MechanismConfiguration.attributeDefinitionSet());
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return new DefaultElementDefinition<>(serializeMechanism(attributes.attribute(SASL_MECHANISM).get()));
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
   public Builder<?> read(MechanismConfiguration template) {
      this.attributes.read(template.attributes());
      return this;
   }

   @Override
   public void validate() {
   }
}
