package org.infinispan.loaders.remote.wrapper;

import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.remote.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * HotRodEntryWrapper.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class HotRodEntryWrapper implements EntryWrapper<byte[], byte[]> {
   private static final Log log = LogFactory.getLog(HotRodEntryWrapper.class, Log.class);

   @Override
   public byte[] wrapKey(Object key) throws CacheLoaderException {
      return (byte[]) key;
   }

   @Override
   public byte[] wrapValue(MetadataValue<?> value) throws CacheLoaderException {
      Object v = value.getValue();
      if (v instanceof byte[]) {
         return (byte[]) v;
      } else {
         throw log.unsupportedValueFormat(v != null ? v.getClass().getName() : "null");
      }
   }

}
