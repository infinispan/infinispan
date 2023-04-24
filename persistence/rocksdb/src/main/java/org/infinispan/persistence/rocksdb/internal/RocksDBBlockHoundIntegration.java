package org.infinispan.persistence.rocksdb.internal;

import org.kohsuke.MetaInfServices;
import org.rocksdb.RocksDB;

import reactor.blockhound.BlockHound;
import reactor.blockhound.integration.BlockHoundIntegration;

@MetaInfServices
public class RocksDBBlockHoundIntegration implements BlockHoundIntegration {
   @Override
   public void applyTo(BlockHound.Builder builder) {
      try {
         Class.forName("org.rocksdb.RocksDB");
         builder.markAsBlocking(RocksDB.class, "get", "(Lorg/rocksdb/ColumnFamilyHandle;[B)[B");
         builder.markAsBlocking(RocksDB.class, "put", "(Lorg/rocksdb/ColumnFamilyHandle;[B[B)V");
         builder.markAsBlocking(RocksDB.class, "delete", "(Lorg/rocksdb/ColumnFamilyHandle;[B)V");
         builder.markAsBlocking(RocksDB.class, "write", "(Lorg/rocksdb/WriteOptions;Lorg/rocksdb/WriteBatch;)V");
         builder.markAsBlocking(RocksDB.class, "close", "()V");
         builder.markAsBlocking(RocksDB.class, "open", "(Lorg/rocksdb/DBOptions;Ljava/lang/String;Ljava/util/List;Ljava/util/List;)Lorg/rocksdb/RocksDB;");
      } catch (ClassNotFoundException e) {
         // Skipping rocks db checks as not in classpath
      }
   }
}
