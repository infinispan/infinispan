package org.infinispan.client.hotrod.impl.operations;

import org.infinispan.client.hotrod.VersionedMetadata;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

public record GetStreamStartResponse(int id, boolean complete, ByteBuf value, VersionedMetadata metadata, Channel channel) {
}
