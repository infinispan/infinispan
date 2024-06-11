package org.infinispan.client.hotrod.impl.multimap.operations;

import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.operations.AbstractKeyOperation;
import org.infinispan.client.hotrod.impl.protocol.Codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

public abstract class AbstractMultimapKeyOperation<V> extends AbstractKeyOperation<V> {

    protected final boolean supportsDuplicates;

    public AbstractMultimapKeyOperation(InternalRemoteCache<?, ?> remoteCache, byte[] keyBytes, boolean supportsDuplicates) {
        super(remoteCache, keyBytes);
        this.supportsDuplicates = supportsDuplicates;
    }

    @Override
    public void writeOperationRequest(Channel channel, ByteBuf buf, Codec codec) {
        super.writeOperationRequest(channel, buf, codec);
        codec.writeMultimapSupportDuplicates(buf, supportsDuplicates);
    }
}
