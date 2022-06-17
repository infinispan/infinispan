package org.infinispan.hotrod.impl.multimap.operations;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.infinispan.api.common.CacheEntryExpiration;
import org.infinispan.api.common.CacheOptions;
import org.infinispan.api.common.CacheWriteOptions;
import org.infinispan.hotrod.impl.DataFormat;
import org.infinispan.hotrod.impl.operations.AbstractKeyValueOperation;
import org.infinispan.hotrod.impl.operations.OperationContext;
import org.infinispan.hotrod.impl.protocol.Codec;
import org.infinispan.hotrod.impl.transport.netty.ByteBufUtil;

public abstract class AbstractMultimapKeyValueOperation<K, T> extends AbstractKeyValueOperation<K, T> {

    protected final boolean supportsDuplicates;

    protected AbstractMultimapKeyValueOperation(OperationContext operationContext, short requestCode, short responseCode, K key, byte[] keyBytes,
                                                byte[] value,
                                                CacheOptions options,
                                                DataFormat dataFormat, boolean supportsDuplicates) {
        super(operationContext, requestCode, responseCode, key, keyBytes, value, options, dataFormat);
        this.supportsDuplicates = supportsDuplicates;
    }

    @Override
    protected void executeOperation(Channel channel) {
        scheduleRead(channel);
        sendKeyValueOperation(channel);
    }

    protected void sendKeyValueOperation(Channel channel) {
        Codec codec = operationContext.getCodec();
        CacheEntryExpiration.Impl expiration = (CacheEntryExpiration.Impl) ((CacheWriteOptions) options).expiration();
        ByteBuf buf = channel.alloc().buffer(codec.estimateHeaderSize(header) + keyBytes.length +
                codec.estimateExpirationSize(expiration) + value.length + codec.estimateSizeMultimapSupportsDuplicated());
        codec.writeHeader(buf, header);
        ByteBufUtil.writeArray(buf, keyBytes);
        codec.writeExpirationParams(buf, expiration);
        ByteBufUtil.writeArray(buf, value);
        codec.writeMultimapSupportDuplicates(buf, supportsDuplicates);
        channel.writeAndFlush(buf);
    }
}
