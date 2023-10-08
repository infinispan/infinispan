package org.infinispan.tools.store.migrator.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.jboss.marshalling.commons.StreamingMarshaller;
import org.infinispan.persistence.jdbc.common.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.impl.table.TableManager;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.PersistenceException;

/**
 * @author Ryan Emerson
 * @since 9.0
 */
class BinaryJdbcIterator extends AbstractJdbcEntryIterator {
   private Iterator<MarshallableEntry> iterator = Collections.emptyIterator();

   BinaryJdbcIterator(ConnectionFactory connectionFactory, TableManager tableManager, Marshaller marshaller) {
      super(connectionFactory, tableManager, marshaller);
   }

   @Override
   public boolean hasNext() {
      return iterator.hasNext() || rowIndex < numberOfRows;
   }

   @Override
   public MarshallableEntry next() {
      if (!iterator.hasNext()) {
         iterator = getNextBucketIterator();
      }
      rowIndex++;
      return iterator.next();
   }

   private Iterator<MarshallableEntry> getNextBucketIterator() {
      try {
         if (rs.next()) {
            InputStream inputStream = rs.getBinaryStream(1);
            Map<Object, MarshallableEntry> bucketEntries = unmarshallBucketEntries(inputStream);
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

   private Map<Object, MarshallableEntry> unmarshallBucketEntries(InputStream inputStream) {
      try {
         return (Map<Object, MarshallableEntry>) ((StreamingMarshaller) marshaller).objectFromInputStream(inputStream);
      } catch (IOException e) {
         throw new PersistenceException("I/O error while unmarshalling from stream", e);
      } catch (ClassNotFoundException e) {
         throw new PersistenceException(e);
      }
   }
}
