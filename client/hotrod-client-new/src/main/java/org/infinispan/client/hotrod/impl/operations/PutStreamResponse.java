package org.infinispan.client.hotrod.impl.operations;

import io.netty.channel.Channel;

public record PutStreamResponse(int id, Channel channel) {
}
