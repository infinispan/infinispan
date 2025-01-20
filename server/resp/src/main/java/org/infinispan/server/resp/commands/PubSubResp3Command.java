package org.infinispan.server.resp.commands;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.SubscriberHandler;

import io.netty.channel.ChannelHandlerContext;

public interface PubSubResp3Command extends BaseResp3Command {
   CompletionStage<RespRequestHandler> perform(SubscriberHandler handler, ChannelHandlerContext ctx, List<byte[]> arguments);
}
