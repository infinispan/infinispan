package org.infinispan.client.hotrod.impl.multimap.operations;

import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.ClientStatistics;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.operations.AbstractKeyOperation;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

public abstract class AbstractMultimapKeyOperation<V> extends AbstractKeyOperation<V> {

    protected final boolean supportsDuplicates;

    public AbstractMultimapKeyOperation(short requestCode, short responseCode, Codec codec, ChannelFactory channelFactory,
                                        Object key, byte[] keyBytes, byte[] cacheName, AtomicReference<ClientTopology> clientTopology, int flags,
                                        Configuration cfg, DataFormat dataFormat, ClientStatistics clientStatistics, boolean supportsDuplicates) {
        super(requestCode, responseCode, codec, channelFactory, key, keyBytes, cacheName, clientTopology,
                flags, cfg, dataFormat, clientStatistics, null);
        this.supportsDuplicates = supportsDuplicates;
    }

    @Override
    protected void executeOperation(Channel channel) {
        scheduleRead(channel);
        sendArrayOperation(channel, keyBytes);
    }

    @Override
    protected void sendArrayOperation(Channel channel, byte[] array) {
        ByteBuf buf = channel.alloc().buffer(codec.estimateHeaderSize(header) + ByteBufUtil.estimateArraySize(array) + codec.estimateSizeMultimapSupportsDuplicated());

        codec.writeHeader(buf, header);
        ByteBufUtil.writeArray(buf, array);

        codec.writeMultimapSupportDuplicates(buf, supportsDuplicates);
        channel.writeAndFlush(buf);
    }
}
