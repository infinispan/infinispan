package org.infinispan.tools.store.migrator.jdbc;

import java.io.InputStream;

import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.table.management.TableManager;
import org.infinispan.persistence.keymappers.TwoWayKey2StringMapper;
import org.infinispan.persistence.spi.MarshallableEntry;

/**
 * @author Ryan Emerson
 * @since 10.0
 */
class StringJdbcIterator10 extends AbstractStringJdbcIterator {

   StringJdbcIterator10(ConnectionFactory connectionFactory, TableManager tableManager, StreamingMarshaller marshaller,
                       TwoWayKey2StringMapper key2StringMapper) {
      super(connectionFactory, tableManager, marshaller, key2StringMapper);
   }

   @Override
   MarshallableEntry readMarshalledEntry(Object key, InputStream is) {
      return entryFactory.create(key, unmarshall(is));
   }
}
