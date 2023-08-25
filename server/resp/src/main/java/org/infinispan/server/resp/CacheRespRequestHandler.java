package org.infinispan.server.resp;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.MediaType;

public class CacheRespRequestHandler extends RespRequestHandler {
   protected AdvancedCache<byte[], byte[]> cache;

   protected CacheRespRequestHandler(RespServer respServer) {
      super(respServer);
      setCache(respServer.getCache()
            .withMediaType(MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_OCTET_STREAM));
   }

   public AdvancedCache<byte[], byte[]> cache() {
      return cache;
   }

   protected void setCache(AdvancedCache<byte[], byte[]> cache) {
      this.cache = cache;
   }
}
