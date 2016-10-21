package org.infinispan.persistence.rocksdb.configuration;

public enum CompressionType {
   NONE,
   SNAPPY,
   ZLIB,
   BZLIB2,
   LZ4,
   LZ4HC
}
