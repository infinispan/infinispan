package org.infinispan.server.memcached.logging;

import java.time.temporal.Temporal;

/**
 * @since 15.0
 **/
public abstract class Header {
   protected final Temporal requestStart;
   protected final int requestBytes;
   protected final String principalName;

   protected Header(Temporal requestStart, int requestBytes, String principalName) {
      this.requestStart = requestStart;
      this.requestBytes = requestBytes;
      this.principalName = principalName;
   }

   public abstract String getOp();

   public abstract String getProtocol();

   public abstract Object getKey();
}
