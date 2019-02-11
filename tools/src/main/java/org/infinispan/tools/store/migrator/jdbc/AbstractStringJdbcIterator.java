package org.infinispan.tools.store.migrator.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.NoSuchElementException;

import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.impl.table.TableManager;
import org.infinispan.persistence.keymappers.TwoWayKey2StringMapper;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.MarshallableEntryFactory;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.tools.store.migrator.marshaller.SerializationConfigUtil;

/**
 * @author Ryan Emerson
 * @since 10.0
 */
abstract class AbstractStringJdbcIterator extends AbstractJdbcEntryIterator {

   final TwoWayKey2StringMapper key2StringMapper;
   final MarshallableEntryFactory entryFactory;

   AbstractStringJdbcIterator(ConnectionFactory connectionFactory, TableManager tableManager, StreamingMarshaller marshaller,
                              TwoWayKey2StringMapper key2StringMapper) {
      super(connectionFactory, tableManager, marshaller);
      this.key2StringMapper = key2StringMapper;
      this.entryFactory = SerializationConfigUtil.getEntryFactory(marshaller);
   }

   abstract MarshallableEntry readMarshalledEntry(Object key, InputStream is);

   @Override
   public final boolean hasNext() {
      return rowIndex < numberOfRows;
   }

   @Override
   public final MarshallableEntry next() {
      try {
         if (rs.next()) {
            rowIndex++;
            Object key = key2StringMapper.getKeyMapping(rs.getString(2));
            return readMarshalledEntry(key, rs.getBinaryStream(1));
         } else {
            close();
            throw new NoSuchElementException();
         }
      } catch (SQLException e) {
         throw new PersistenceException("SQL error while fetching all StoredEntries", e);
      }
   }

   @SuppressWarnings("unchecked")
   <T> T unmarshall(InputStream inputStream) throws PersistenceException {
      try {
         return (T) marshaller.objectFromInputStream(inputStream);
      } catch (IOException e) {
         throw new PersistenceException("I/O error while unmarshalling from stream", e);
      } catch (ClassNotFoundException e) {
         throw new PersistenceException(e);
      }
   }
}
