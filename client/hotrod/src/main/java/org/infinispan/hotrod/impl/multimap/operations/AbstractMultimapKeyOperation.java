package org.infinispan.hotrod.impl.multimap.operations;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.infinispan.api.common.CacheOptions;
import org.infinispan.hotrod.impl.DataFormat;
import org.infinispan.hotrod.impl.operations.AbstractKeyOperation;
import org.infinispan.hotrod.impl.operations.OperationContext;
import org.infinispan.hotrod.impl.protocol.Codec;
import org.infinispan.hotrod.impl.transport.netty.ByteBufUtil;

public abstract class AbstractMultimapKeyOperation<K, T> extends AbstractKeyOperation<K, T> {

    protected final boolean supportsDuplicates;

    protected AbstractMultimapKeyOperation(OperationContext operationContext, short requestCode, short responseCode,
                                           K key, byte[] keyBytes, CacheOptions options,
                                           DataFormat dataFormat, boolean supportsDuplicates) {
        super(operationContext, requestCode, responseCode, key, keyBytes, options, dataFormat);
        this.supportsDuplicates = supportsDuplicates;
    }

    @Override
    public void executeOperation(Channel channel) {
        scheduleRead(channel);
        sendArrayOperation(channel, keyBytes);
    }

    @Override
    protected void sendArrayOperation(Channel channel, byte[] array) {
        Codec codec = operationContext.getCodec();
        ByteBuf buf = channel.alloc().buffer(codec.estimateHeaderSize(header) + ByteBufUtil.estimateArraySize(array) + codec.estimateSizeMultimapSupportsDuplicated() );

        codec.writeHeader(buf, header);
        ByteBufUtil.writeArray(buf, array);
        codec.writeMultimapSupportDuplicates(buf, supportsDuplicates);
        channel.writeAndFlush(buf);
    }
}
