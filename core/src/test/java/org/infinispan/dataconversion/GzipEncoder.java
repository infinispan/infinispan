package org.infinispan.dataconversion;

import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.MediaType;

public class GzipEncoder implements Encoder {

   @Override
   public Object toStorage(Object content) {
      assert content instanceof String;
      return Compression.GZIP.compress(content.toString());
   }

   @Override
   public Object fromStorage(Object content) {
      assert content instanceof byte[];
      return Compression.GZIP.decompress((byte[]) content);
   }

   @Override
   public MediaType getStorageFormat() {
      return MediaType.fromString("application/gzip");
   }

   @Override
   public boolean isStorageFormatFilterable() {
      return false;
   }

   @Override
   public short id() {
      return 10000;
   }
}
