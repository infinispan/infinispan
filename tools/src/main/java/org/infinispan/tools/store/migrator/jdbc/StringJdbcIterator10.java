package org.infinispan.tools.store.migrator.jdbc;

import java.io.InputStream;

import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.persistence.jdbc.common.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.impl.table.TableManager;
import org.infinispan.persistence.keymappers.TwoWayKey2StringMapper;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.MarshalledValue;

/**
 * @author Ryan Emerson
 * @since 10.0
 */
class StringJdbcIterator10 extends AbstractStringJdbcIterator {

   StringJdbcIterator10(ConnectionFactory connectionFactory, TableManager tableManager, Marshaller marshaller,
                       TwoWayKey2StringMapper key2StringMapper) {
      super(connectionFactory, tableManager, marshaller, key2StringMapper);
   }

   @Override
   MarshallableEntry readMarshalledEntry(Object key, InputStream is) {
      return entryFactory.create(key, (MarshalledValue) unmarshall(is));
   }
}
