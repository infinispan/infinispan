package org.infinispan.server.resp.filter;

import java.nio.charset.StandardCharsets;

import org.infinispan.filter.KeyValueFilterConverter;
import org.infinispan.filter.ParamKeyValueFilterConverterFactory;

/**
 * @since 15.0
 **/
public class GlobMatchFilterConverterFactory implements ParamKeyValueFilterConverterFactory<byte[], byte[], byte[]> {

   private final boolean returnValue;

   public GlobMatchFilterConverterFactory() {
      this(false);
   }

   public GlobMatchFilterConverterFactory(boolean returnValue) {
      this.returnValue = returnValue;
   }

   @Override
   public KeyValueFilterConverter<byte[], byte[], byte[]> getFilterConverter(Object[] params) {
      return create((byte[]) params[0], returnValue);
   }

   @Override
   public boolean binaryParam() {
      return true;
   }

   static KeyValueFilterConverter<byte[], byte[], byte[]> create(byte[] params, boolean returnValue) {
      return new GlobMatchFilterConverter<>(new String(params, StandardCharsets.UTF_8), returnValue);
   }
}
