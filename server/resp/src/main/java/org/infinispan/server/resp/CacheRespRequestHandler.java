package org.infinispan.server.resp;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.DefaultTranscoder;
import org.infinispan.commons.dataconversion.MediaType;

public class CacheRespRequestHandler extends RespRequestHandler {
   private AdvancedCache<byte[], byte[]> cache;

   protected CacheRespRequestHandler(RespServer respServer, AdvancedCache<byte[], byte[]> cache) {
      super(respServer);
      if (cache != null)
         setCache(cache);
   }

   public AdvancedCache<byte[], byte[]> cache() {
      if (cache == null)
         setCache(respServer().getCache());
      return cache;
   }

   public AdvancedCache<byte[], Object> getObjCache() {
      return (AdvancedCache) cache();
   }

   public <V> AdvancedCache<byte[], V> typedCache(MediaType valueMediaType) {
      return cache.withMediaType(RespServer.RESP_KEY_MEDIA_TYPE, DefaultTranscoder.useGlobalMarshaller(valueMediaType));
   }

   protected void setCache(AdvancedCache<byte[], byte[]> cache) {
      this.cache = cache.withMediaType(MediaType.APPLICATION_OCTET_STREAM, DefaultTranscoder.useGlobalMarshaller(MediaType.APPLICATION_OBJECT));
   }
}
