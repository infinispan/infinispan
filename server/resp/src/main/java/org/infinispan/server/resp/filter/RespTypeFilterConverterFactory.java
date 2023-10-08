package org.infinispan.server.resp.filter;

import org.infinispan.filter.KeyValueFilterConverter;
import org.infinispan.filter.ParamKeyValueFilterConverterFactory;
import org.infinispan.server.resp.RespTypes;

/**
 * @since 15.0
 */
public class RespTypeFilterConverterFactory implements ParamKeyValueFilterConverterFactory<byte[], Object, Object> {

   @Override
   public KeyValueFilterConverter<byte[], Object, Object> getFilterConverter(Object[] params) {
      return create((byte[]) params[0]);
   }

   @Override
   public boolean binaryParam() {
      return true;
   }

   static KeyValueFilterConverter<byte[], Object, Object> create(byte[] params) {
      byte ordinal = params[0];
      return new RespTypeFilterConverter<>(RespTypes.fromOrdinal(ordinal));
   }
}
