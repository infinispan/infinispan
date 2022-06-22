package org.infinispan.client.hotrod.impl.multimap.operations;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.exceptions.InvalidResponseException;
import org.infinispan.client.hotrod.impl.ClientStatistics;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.operations.AbstractKeyValueOperation;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

public abstract class AbstractMultimapKeyValueOperation<T> extends AbstractKeyValueOperation<T> {

    protected final boolean supportsDuplicates;

    protected AbstractMultimapKeyValueOperation(short requestCode, short responseCode, Codec codec, ChannelFactory channelFactory, Object key, byte[] keyBytes, byte[] cacheName,
                                                AtomicReference<ClientTopology> clientTopology, int flags, Configuration cfg, byte[] value,
                                                long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit,
                                                DataFormat dataFormat, ClientStatistics clientStatistics, boolean supportsDuplicates) {
        super(requestCode, responseCode, codec, channelFactory, key, keyBytes, cacheName, clientTopology, flags, cfg, value, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit, dataFormat, clientStatistics, null);
        this.supportsDuplicates = supportsDuplicates;
    }

    @Override
    protected void executeOperation(Channel channel) {
        scheduleRead(channel);
        sendKeyValueOperation(channel);
    }

    @Override
    public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
        if (!HotRodConstants.isSuccess(status)) {
            throw new InvalidResponseException("Unexpected response status: " + Integer.toHexString(status));
        }
        complete(null);
    }

    @Override
    protected void sendKeyValueOperation(Channel channel) {
        ByteBuf buf = channel.alloc().buffer(codec.estimateHeaderSize(header) + keyBytes.length +
                codec.estimateExpirationSize(lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit) + value.length +
                codec.estimateSizeMultimapSupportsDuplicated());
        codec.writeHeader(buf, header);
        ByteBufUtil.writeArray(buf, keyBytes);
        codec.writeExpirationParams(buf, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit);
        ByteBufUtil.writeArray(buf, value);
        codec.writeMultimapSupportDuplicates(buf, supportsDuplicates);
        channel.writeAndFlush(buf);
    }
}
