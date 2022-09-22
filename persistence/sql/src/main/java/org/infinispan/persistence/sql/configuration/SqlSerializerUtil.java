package org.infinispan.persistence.sql.configuration;

import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.persistence.jdbc.common.configuration.Element;

class SqlSerializerUtil {

   static void writeSchemaElement(ConfigurationWriter writer, AbstractSchemaJdbcConfiguration configuration) {
      SchemaJdbcConfiguration schemaConfig = configuration.schema();
      if (schemaConfig.attributes().isModified()) {
         writer.writeStartElement(Element.SCHEMA);
         schemaConfig.attributes().write(writer);
         writer.writeEndElement();
      }
   }
}
