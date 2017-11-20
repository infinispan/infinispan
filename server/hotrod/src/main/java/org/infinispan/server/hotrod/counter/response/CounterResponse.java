package org.infinispan.server.hotrod.counter.response;

import io.netty.buffer.ByteBuf;

/**
 * An interface that counter's response has to implement.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public interface CounterResponse {

   void writeTo(ByteBuf buffer);

}
