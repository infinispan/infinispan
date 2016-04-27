package org.infinispan.persistence.jdbc.configuration;

import javax.xml.stream.XMLStreamException;

import org.infinispan.configuration.serializing.ConfigurationSerializer;
import org.infinispan.configuration.serializing.XMLExtendedStreamWriter;

/**
 * JdbcBinaryStoreConfigurationSerializer.
 *
 * @author Tristan Tarrant
 * @since 9.0
 */
public class JdbcBinaryStoreConfigurationSerializer extends AbstractJdbcStoreConfigurationSerializer implements ConfigurationSerializer<JdbcBinaryStoreConfiguration> {

   @Override
   public void serialize(XMLExtendedStreamWriter writer, JdbcBinaryStoreConfiguration configuration) throws XMLStreamException {
      writer.writeStartElement(Element.BINARY_KEYED_JDBC_STORE);
      writeJdbcStoreAttributes(writer, configuration);
      writeCommonStoreSubAttributes(writer, configuration);
      writeJDBCStoreTable(writer, Element.BINARY_KEYED_TABLE, configuration.table());
      writeJDBCStoreConnection(writer, configuration);
      writeCommonStoreElements(writer, configuration);
      writer.writeEndElement();
   }
}
