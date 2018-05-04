package org.infinispan.tools.store.migrator;

/**
 * @author Ryan Emerson
 * @since 9.0
 */
public enum StoreType {
   JDBC_BINARY,
   JDBC_MIXED,
   JDBC_STRING,
   LEVELDB,
   ROCKSDB,
   SINGLE_FILE_STORE,
   SOFT_INDEX_FILE_STORE
}
