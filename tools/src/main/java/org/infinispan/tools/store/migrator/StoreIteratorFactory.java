package org.infinispan.tools.store.migrator;

import static org.infinispan.tools.store.migrator.Element.SOURCE;

import java.util.Properties;

import org.infinispan.tools.store.migrator.jdbc.JdbcStoreReader;

class StoreIteratorFactory {

   static StoreIterator get(Properties properties) {
      StoreProperties props = new StoreProperties(SOURCE, properties);
      switch (props.storeType()) {
         case BINARY:
         case MIXED:
         case STRING:
            return new JdbcStoreReader(props);
      }
      return null;
   }
}
