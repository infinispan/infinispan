package org.infinispan.tools.store.migrator.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.impl.table.TableManager;
import org.infinispan.persistence.spi.PersistenceException;

/**
 * @author Ryan Emerson
 * @since 9.0
 */
class BinaryJdbcIterator extends AbstractJdbcEntryIterator {
   private Iterator<MarshalledEntry> iterator = Collections.emptyIterator();

   BinaryJdbcIterator(ConnectionFactory connectionFactory, TableManager tableManager, StreamingMarshaller marshaller) {
      super(connectionFactory, tableManager, marshaller);
   }

   @Override
   public boolean hasNext() {
      return iterator.hasNext() || rowIndex < numberOfRows;
   }

   @Override
   public MarshalledEntry next() {
      if (!iterator.hasNext()) {
         iterator = getNextBucketIterator();
      }
      rowIndex++;
      return iterator.next();
   }

   private Iterator<MarshalledEntry> getNextBucketIterator() {
      try {
         if (rs.next()) {
            InputStream inputStream = rs.getBinaryStream(1);
            Map<Object, MarshalledEntry> bucketEntries = unmarshallBucketEntries(inputStream);
            numberOfRows += bucketEntries.size() - 1; // Guaranteed that bucket size will never be 0
            return bucketEntries.values().iterator();
         } else {
            close();
            throw new NoSuchElementException();
         }
      } catch (SQLException e) {
         throw new PersistenceException("SQL error while fetching all StoredEntries", e);
      }
   }

   private Map<Object, MarshalledEntry> unmarshallBucketEntries(InputStream inputStream) {
      try {
         return (Map<Object, MarshalledEntry>) marshaller.objectFromInputStream(inputStream);
      } catch (IOException e) {
         throw new PersistenceException("I/O error while unmarshalling from stream", e);
      } catch (ClassNotFoundException e) {
         throw new PersistenceException(e);
      }
   }
}
