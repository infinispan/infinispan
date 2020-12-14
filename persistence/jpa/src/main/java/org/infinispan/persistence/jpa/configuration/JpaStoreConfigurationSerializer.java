package org.infinispan.persistence.jpa.configuration;

import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.commons.util.Version;
import org.infinispan.configuration.serializing.AbstractStoreSerializer;
import org.infinispan.configuration.serializing.ConfigurationSerializer;

/**
 * JpaStoreConfigurationSerializer.
 *
 * @since 9.0
 * @author Tristan Tarrant
 */
public class JpaStoreConfigurationSerializer extends AbstractStoreSerializer implements ConfigurationSerializer<JpaStoreConfiguration> {

   @Override
   public void serialize(ConfigurationWriter writer, JpaStoreConfiguration configuration) {
      writer.writeStartElement(Element.JPA_STORE);
      writer.writeDefaultNamespace(JpaStoreConfigurationParser.NAMESPACE + Version.getMajorMinor());
      configuration.attributes().write(writer);
      writeCommonStoreSubAttributes(writer, configuration);
      writeCommonStoreElements(writer, configuration);
      writer.writeEndElement();
   }
}
