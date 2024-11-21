package org.infinispan.client.hotrod.impl.operations;

import io.netty.buffer.ByteBuf;

public record GetStreamNextResponse(ByteBuf value, boolean complete) {
}
