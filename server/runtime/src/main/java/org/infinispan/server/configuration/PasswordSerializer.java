package org.infinispan.server.configuration;

import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeSerializer;

/**
 * @since 10.0
 */
public class PasswordSerializer extends AttributeSerializer<Object, ConfigurationInfo, ConfigurationBuilderInfo> {

   public static final PasswordSerializer INSTANCE = new PasswordSerializer();

   private PasswordSerializer() {
   }

   @Override
   public Object getSerializationValue(Attribute attribute, ConfigurationInfo configurationElement) {
      return "***";
   }
}
