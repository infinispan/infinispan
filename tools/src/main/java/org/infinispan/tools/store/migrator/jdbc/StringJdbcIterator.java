package org.infinispan.tools.store.migrator.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.NoSuchElementException;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.table.management.TableManager;
import org.infinispan.persistence.keymappers.TwoWayKey2StringMapper;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.marshall.core.MarshalledEntryFactory;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.tools.store.migrator.marshaller.SerializationConfigUtil;
import org.infinispan.util.KeyValuePair;

/**
 * @author Ryan Emerson
 * @since 9.0
 */
class StringJdbcIterator extends AbstractJdbcEntryIterator {

   private final TwoWayKey2StringMapper key2StringMapper;
   private final MarshalledEntryFactory entryFactory;

   StringJdbcIterator(ConnectionFactory connectionFactory, TableManager tableManager, StreamingMarshaller marshaller,
                      TwoWayKey2StringMapper key2StringMapper) {
      super(connectionFactory, tableManager, marshaller);
      this.key2StringMapper = key2StringMapper;
      this.entryFactory = SerializationConfigUtil.getEntryFactory(marshaller);
   }

   @Override
   public boolean hasNext() {
      return rowIndex < numberOfRows;
   }

   @Override
   public MarshalledEntry next() {
      try {
         if (rs.next()) {
            rowIndex++;
            Object key = key2StringMapper.getKeyMapping(rs.getString(2));
            KeyValuePair<ByteBuffer, ByteBuffer> icv = unmarshall(rs.getBinaryStream(1));
            return entryFactory.newMarshalledEntry(key, icv.getKey(), icv.getValue());
         } else {
            close();
            throw new NoSuchElementException();
         }
      } catch (SQLException e) {
         throw new PersistenceException("SQL error while fetching all StoredEntries", e);
      }
   }

   @SuppressWarnings("unchecked")
   private KeyValuePair<ByteBuffer, ByteBuffer> unmarshall(InputStream inputStream) throws PersistenceException {
      try {
         return (KeyValuePair<ByteBuffer, ByteBuffer>) marshaller.objectFromInputStream(inputStream);
      } catch (IOException e) {
         throw new PersistenceException("I/O error while unmarshalling from stream", e);
      } catch (ClassNotFoundException e) {
         throw new PersistenceException(e);
      }
   }
}
