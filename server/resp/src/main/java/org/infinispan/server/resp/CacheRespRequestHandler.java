package org.infinispan.server.resp;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.MediaType;

public class CacheRespRequestHandler extends RespRequestHandler {
   private AdvancedCache<byte[], byte[]> cache;

   protected CacheRespRequestHandler(RespServer respServer, AdvancedCache<byte[], byte[]> cache) {
      super(respServer);
      if (cache != null)
         setCache(cache.withMediaType(MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_OCTET_STREAM));
   }

   public AdvancedCache<byte[], byte[]> cache() {
      if (cache == null)
         setCache(respServer().getCache().withMediaType(MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_OCTET_STREAM));
      return cache;
   }

   @SuppressWarnings("unchecked")
   public <V> AdvancedCache<byte[], V> typedCache(MediaType valueMediaType) {
      AdvancedCache<byte[], ?> c = cache;
      if (cache.getValueDataConversion().getRequestMediaType().match(valueMediaType)) {
         return (AdvancedCache<byte[], V>) c;
      }

      return cache.withMediaType(RespServer.RESP_KEY_MEDIA_TYPE, valueMediaType);
   }

   protected void setCache(AdvancedCache<byte[], byte[]> cache) {
      this.cache = cache;
   }
}
