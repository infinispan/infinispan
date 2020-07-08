package org.infinispan.persistence.jdbc.configuration;

import javax.xml.stream.XMLStreamException;

import org.infinispan.commons.util.Version;
import org.infinispan.configuration.serializing.ConfigurationSerializer;
import org.infinispan.configuration.serializing.XMLExtendedStreamWriter;

/**
 * JdbcStringBasedStoreConfigurationSerializer.
 *
 * @author Tristan Tarrant
 * @since 9.0
 */
public class JdbcStringBasedStoreConfigurationSerializer extends AbstractJdbcStoreConfigurationSerializer implements ConfigurationSerializer<JdbcStringBasedStoreConfiguration> {

   @Override
   public void serialize(XMLExtendedStreamWriter writer, JdbcStringBasedStoreConfiguration configuration) throws XMLStreamException {
      writer.writeStartElement(Element.STRING_KEYED_JDBC_STORE);
      writer.writeDefaultNamespace(JdbcStoreConfigurationParser.NAMESPACE + Version.getMajorMinor());
      writeJdbcStoreAttributes(writer, configuration);
      writeCommonStoreSubAttributes(writer, configuration);
      writeJDBCStoreTable(writer, Element.STRING_KEYED_TABLE, configuration.table());
      writeJDBCStoreConnection(writer, configuration);
      writeCommonStoreElements(writer, configuration);
      writer.writeEndElement();
   }


}
