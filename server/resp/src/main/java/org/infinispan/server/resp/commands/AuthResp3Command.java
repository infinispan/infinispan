package org.infinispan.server.resp.commands;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.Resp3AuthHandler;
import org.infinispan.server.resp.RespRequestHandler;

import io.netty.channel.ChannelHandlerContext;

public interface AuthResp3Command {
   CompletionStage<RespRequestHandler> perform(Resp3AuthHandler handler, ChannelHandlerContext ctx, List<byte[]> arguments);
}
