package org.infinispan.tools.store.migrator.jdbc;

import java.io.InputStream;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.impl.table.TableManager;
import org.infinispan.persistence.keymappers.TwoWayKey2StringMapper;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.util.KeyValuePair;

/**
 * @author Ryan Emerson
 * @since 9.0
 */
class StringJdbcIterator extends AbstractStringJdbcIterator {

   StringJdbcIterator(ConnectionFactory connectionFactory, TableManager tableManager, StreamingMarshaller marshaller,
                      TwoWayKey2StringMapper key2StringMapper) {
      super(connectionFactory, tableManager, marshaller, key2StringMapper);
   }

   @Override
   MarshallableEntry readMarshalledEntry(Object key, InputStream is) {
      KeyValuePair<ByteBuffer, ByteBuffer> icv = unmarshall(is);
      ByteBuffer buf = icv.getKey();
      try {
         Object value = marshaller.objectFromByteBuffer(buf.getBuf(), buf.getOffset(), buf.getLength());
         return entryFactory.create(key, value);
      } catch (Exception e) {
         throw new PersistenceException(e);
      }
   }
}
