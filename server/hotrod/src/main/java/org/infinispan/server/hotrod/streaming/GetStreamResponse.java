package org.infinispan.server.hotrod.streaming;

import io.netty.buffer.ByteBuf;

public record GetStreamResponse(int id, ByteBuf value, boolean complete) { }
