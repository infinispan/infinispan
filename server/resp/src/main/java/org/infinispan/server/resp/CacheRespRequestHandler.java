package org.infinispan.server.resp;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.MediaType;

public class CacheRespRequestHandler extends RespRequestHandler {
   protected final RespServer respServer;
   protected AdvancedCache<byte[], byte[]> cache;

   protected CacheRespRequestHandler(RespServer respServer) {
      this.respServer = respServer;
      this.cache = respServer.getCache()
            .withMediaType(MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_OCTET_STREAM);
   }
}
