package org.infinispan.server.resp.filter;

import java.nio.charset.StandardCharsets;

import org.infinispan.filter.KeyValueFilterConverter;
import org.infinispan.filter.ParamKeyValueFilterConverterFactory;

/**
 * @since 15.0
 **/
public class GlobMatchFilterConverterFactory implements ParamKeyValueFilterConverterFactory<byte[], byte[], byte[]> {
   @Override
   public KeyValueFilterConverter<byte[], byte[], byte[]> getFilterConverter(Object[] params) {
      return new GlobMatchFilterConverter(new String((byte[]) params[0], StandardCharsets.UTF_8));
   }
}
