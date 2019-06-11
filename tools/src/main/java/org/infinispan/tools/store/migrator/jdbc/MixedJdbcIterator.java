package org.infinispan.tools.store.migrator.jdbc;

import java.util.Iterator;

import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.impl.table.TableManager;
import org.infinispan.persistence.keymappers.TwoWayKey2StringMapper;

/**
 * @author Ryan Emerson
 * @since 9.0
 */
class MixedJdbcIterator implements Iterator<MarshallableEntry>, AutoCloseable {
   private BinaryJdbcIterator binaryIt;
   private StringJdbcIterator stringIt;

   MixedJdbcIterator(ConnectionFactory connectionFactory, TableManager binaryTm, TableManager stringTm,
                     Marshaller marshaller, TwoWayKey2StringMapper key2StringMapper) {
      binaryIt = new BinaryJdbcIterator(connectionFactory, binaryTm, marshaller);
      stringIt = new StringJdbcIterator(connectionFactory, stringTm, marshaller, key2StringMapper);
   }

   @Override
   public boolean hasNext() {
      return binaryIt.hasNext() || stringIt.hasNext();
   }

   @Override
   public MarshallableEntry next() {
      if (binaryIt.hasNext())
         return binaryIt.next();
      return stringIt.next();
   }

   @Override
   public void close() {
      binaryIt.close();
      stringIt.close();
   }
}
