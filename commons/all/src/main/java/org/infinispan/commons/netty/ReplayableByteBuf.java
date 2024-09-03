package org.infinispan.commons.netty;

import io.netty.buffer.ByteBuf;

public interface ReplayableByteBuf {

   ByteBuf internal();
}
