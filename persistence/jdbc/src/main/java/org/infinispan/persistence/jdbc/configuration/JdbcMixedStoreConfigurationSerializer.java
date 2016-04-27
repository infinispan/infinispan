package org.infinispan.persistence.jdbc.configuration;

import javax.xml.stream.XMLStreamException;

import org.infinispan.configuration.serializing.ConfigurationSerializer;
import org.infinispan.configuration.serializing.XMLExtendedStreamWriter;

/**
 * JdbcMixedStoreConfigurationSerializer.
 *
 * @author Tristan Tarrant
 * @since 9.0
 */
public class JdbcMixedStoreConfigurationSerializer extends AbstractJdbcStoreConfigurationSerializer implements ConfigurationSerializer<JdbcMixedStoreConfiguration> {

   @Override
   public void serialize(XMLExtendedStreamWriter writer, JdbcMixedStoreConfiguration configuration) throws XMLStreamException {
      writer.writeStartElement(Element.MIXED_KEYED_JDBC_STORE);
      writeJdbcStoreAttributes(writer, configuration);
      writeCommonStoreSubAttributes(writer, configuration);
      writeJDBCStoreTable(writer, Element.STRING_KEYED_TABLE, configuration.stringTable());
      writeJDBCStoreTable(writer, Element.BINARY_KEYED_TABLE, configuration.binaryTable());
      writeJDBCStoreConnection(writer, configuration);
      writeCommonStoreElements(writer, configuration);
      writer.writeEndElement();
   }
}
