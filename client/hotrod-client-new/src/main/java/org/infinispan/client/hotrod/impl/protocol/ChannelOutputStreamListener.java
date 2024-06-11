package org.infinispan.client.hotrod.impl.protocol;

import java.io.IOException;

import io.netty.channel.Channel;

public interface ChannelOutputStreamListener {
   void onClose(Channel channel) throws IOException;
   void onError(Channel channel, Throwable error);
}
