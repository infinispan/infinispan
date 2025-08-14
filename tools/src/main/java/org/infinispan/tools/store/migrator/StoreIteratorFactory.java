package org.infinispan.tools.store.migrator;

import static org.infinispan.tools.store.migrator.Element.SOURCE;

import java.util.Properties;

import org.infinispan.tools.store.migrator.file.SoftIndexFileStoreIterator;
import org.infinispan.tools.store.migrator.jdbc.JdbcStoreReader;
import org.infinispan.tools.store.migrator.rocksdb.RocksDBReader;

class StoreIteratorFactory {

   static StoreIterator get(Properties properties) {
      StoreProperties props = new StoreProperties(SOURCE, properties);
      StoreType type = props.storeType();
      switch (type) {
         case JDBC_BINARY:
         case JDBC_MIXED:
         case JDBC_STRING:
            return new JdbcStoreReader(props);
      }

      if (props.isSegmented()) {
         throw new IllegalArgumentException(String.format("Segmented %s source store not supported", type));
      }

      switch (type) {
         case LEVELDB:
         case ROCKSDB:
            return new RocksDBReader(props);
         case SOFT_INDEX_FILE_STORE:
            return new SoftIndexFileStoreIterator(props);
      }
      return null;
   }
}
