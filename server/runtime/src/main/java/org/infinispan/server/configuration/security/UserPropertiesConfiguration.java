package org.infinispan.server.configuration.security;

import java.io.File;
import java.util.Properties;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.server.Server;
import org.infinispan.server.configuration.Attribute;
import org.infinispan.server.configuration.Element;

public class UserPropertiesConfiguration extends ConfigurationElement<UserPropertiesConfiguration> {
   static final AttributeDefinition<String> DIGEST_REALM_NAME = AttributeDefinition.builder(Attribute.DIGEST_REALM_NAME, null, String.class).build();
   static final AttributeDefinition<String> PATH = AttributeDefinition.builder(Attribute.PATH, null, String.class).build();
   static final AttributeDefinition<Boolean> PLAIN_TEXT = AttributeDefinition.builder(Attribute.PLAIN_TEXT, false, Boolean.class).build();
   static final AttributeDefinition<String> RELATIVE_TO = AttributeDefinition.builder(Attribute.RELATIVE_TO, Server.INFINISPAN_SERVER_CONFIG_PATH, String.class).autoPersist(false).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(UserPropertiesConfiguration.class, DIGEST_REALM_NAME, PATH, PLAIN_TEXT, RELATIVE_TO);
   }

   UserPropertiesConfiguration(AttributeSet attributes) {
      super(Element.USER_PROPERTIES, attributes);
   }

   public String path() {
      return attributes.attribute(PATH).get();
   }

   public String relativeTo() {
      return attributes.attribute(RELATIVE_TO).get();
   }

   public boolean plainText() {
      return attributes.attribute(PLAIN_TEXT).get();
   }

   public String digestRealmName() {
      return attributes.attribute(DIGEST_REALM_NAME).get();
   }

   public File getFile(Properties properties) {
      String path = attributes.attribute(PATH).get();
      String relativeTo = properties.getProperty(attributes.attribute(RELATIVE_TO).get());
      return new File(ParseUtils.resolvePath(path, relativeTo));
   }
}
