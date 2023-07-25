package org.infinispan.server.resp.commands;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.tx.RespTransactionHandler;

import io.netty.channel.ChannelHandlerContext;

public interface TransactionResp3Command {

   CompletionStage<RespRequestHandler> perform(RespTransactionHandler handler, ChannelHandlerContext ctx, List<byte[]> arguments);
}
