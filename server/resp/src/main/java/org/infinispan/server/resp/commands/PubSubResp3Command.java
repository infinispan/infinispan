package org.infinispan.server.resp.commands;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.SubscriberHandler;

import java.util.List;
import java.util.concurrent.CompletionStage;

public interface PubSubResp3Command {
   CompletionStage<RespRequestHandler> perform(SubscriberHandler handler, ChannelHandlerContext ctx, List<byte[]> arguments);
}
