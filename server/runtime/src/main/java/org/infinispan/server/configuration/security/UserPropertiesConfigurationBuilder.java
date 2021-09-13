package org.infinispan.server.configuration.security;

import static org.infinispan.server.configuration.security.UserPropertiesConfiguration.DIGEST_REALM_NAME;
import static org.infinispan.server.configuration.security.UserPropertiesConfiguration.PATH;
import static org.infinispan.server.configuration.security.UserPropertiesConfiguration.PLAIN_TEXT;
import static org.infinispan.server.configuration.security.UserPropertiesConfiguration.RELATIVE_TO;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * @since 10.0
 */
public class UserPropertiesConfigurationBuilder implements Builder<UserPropertiesConfiguration> {
   private final AttributeSet attributes;

   UserPropertiesConfigurationBuilder() {
      attributes = UserPropertiesConfiguration.attributeDefinitionSet();
   }

   public UserPropertiesConfigurationBuilder path(String path) {
      attributes.attribute(PATH).set(path);
      return this;
   }

   public UserPropertiesConfigurationBuilder relativeTo(String relativeTo) {
      attributes.attribute(RELATIVE_TO).set(relativeTo);
      return this;
   }

   public UserPropertiesConfigurationBuilder plainText(boolean plainText) {
      attributes.attribute(PLAIN_TEXT).set(plainText);
      return this;
   }

   public UserPropertiesConfigurationBuilder digestRealmName(String name) {
      attributes.attribute(DIGEST_REALM_NAME).set(name);
      return this;
   }

   public boolean plainText() {
      return attributes.attribute(PLAIN_TEXT).get();
   }

   @Override
   public UserPropertiesConfiguration create() {
      return new UserPropertiesConfiguration(attributes.protect());
   }

   @Override
   public UserPropertiesConfigurationBuilder read(UserPropertiesConfiguration template) {
      attributes.read(template.attributes());
      return this;
   }
}
