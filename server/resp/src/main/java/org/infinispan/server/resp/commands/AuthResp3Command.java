package org.infinispan.server.resp.commands;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.server.resp.Resp3AuthHandler;
import org.infinispan.server.resp.RespRequestHandler;

import java.util.List;
import java.util.concurrent.CompletionStage;

public interface AuthResp3Command {
   CompletionStage<RespRequestHandler> perform(Resp3AuthHandler handler, ChannelHandlerContext ctx, List<byte[]> arguments);
}
