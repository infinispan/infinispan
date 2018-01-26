package org.infinispan.server.hotrod;

import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

import io.netty.channel.Channel;

public class BaseRequestProcessor {
   protected final Channel channel;
   protected final Executor executor;

   BaseRequestProcessor(Channel channel, Executor executor) {
      this.channel = channel;
      this.executor = executor;
   }

   protected void writeException(CacheDecodeContext cdc, Throwable cause) {
      if (cause instanceof CompletionException && cause.getCause() != null) {
         cause = cause.getCause();
      }
      writeResponse(cdc.createExceptionResponse(cause));
   }

   protected void writeSuccess(CacheDecodeContext cdc, byte[] result) {
      writeResponse(cdc.decoder.createSuccessResponse(cdc.header, result));
   }

   protected void writeNotExecuted(CacheDecodeContext cdc, byte[] prev) {
      writeResponse(cdc.decoder.createNotExecutedResponse(cdc.header, prev));
   }

   protected void writeNotExist(CacheDecodeContext cdc) {
      writeResponse(cdc.decoder.createNotExistResponse(cdc.header));
   }

   protected void writeResponse(Response response) {
      ResponseWriting.writeResponse(channel, response);
   }
}
