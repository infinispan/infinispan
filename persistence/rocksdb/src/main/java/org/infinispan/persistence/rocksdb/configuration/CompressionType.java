package org.infinispan.persistence.rocksdb.configuration;

public enum CompressionType {
   NONE(org.rocksdb.CompressionType.NO_COMPRESSION),
   SNAPPY(org.rocksdb.CompressionType.SNAPPY_COMPRESSION),
   ZLIB(org.rocksdb.CompressionType.ZLIB_COMPRESSION),
   BZLIB2(org.rocksdb.CompressionType.BZLIB2_COMPRESSION),
   LZ4(org.rocksdb.CompressionType.LZ4_COMPRESSION),
   LZ4HC(org.rocksdb.CompressionType.LZ4HC_COMPRESSION),
   XPRESS(org.rocksdb.CompressionType.XPRESS_COMPRESSION),
   ZSTD(org.rocksdb.CompressionType.ZSTD_COMPRESSION);

   private final org.rocksdb.CompressionType value;

   CompressionType(org.rocksdb.CompressionType value) {
      this.value = value;
   }

   public org.rocksdb.CompressionType getValue() {
      return value;
   }
}
