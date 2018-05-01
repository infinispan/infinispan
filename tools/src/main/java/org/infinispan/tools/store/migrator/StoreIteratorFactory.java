package org.infinispan.tools.store.migrator;

import static org.infinispan.tools.store.migrator.Element.SOURCE;

import java.util.Properties;

import org.infinispan.tools.store.migrator.jdbc.JdbcStoreReader;
import org.infinispan.tools.store.migrator.rocksdb.RocksDBReader;

class StoreIteratorFactory {

   static StoreIterator get(Properties properties) {
      StoreProperties props = new StoreProperties(SOURCE, properties);
      switch (props.storeType()) {
         case JDBC_BINARY:
         case JDBC_MIXED:
         case JDBC_STRING:
            return new JdbcStoreReader(props);
         case LEVELDB:
         case ROCKSDB:
            return new RocksDBReader(props);
      }
      return null;
   }
}
