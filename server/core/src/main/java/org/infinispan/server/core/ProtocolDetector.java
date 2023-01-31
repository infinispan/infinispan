package org.infinispan.server.core;

import io.netty.handler.codec.ByteToMessageDecoder;

/**
 * @since 15.0
 **/
public abstract class ProtocolDetector extends ByteToMessageDecoder {
   protected final ProtocolServer<?> server;

   protected ProtocolDetector(ProtocolServer<?> server) {
      this.server = server;
   }

   public abstract String getName();

}
