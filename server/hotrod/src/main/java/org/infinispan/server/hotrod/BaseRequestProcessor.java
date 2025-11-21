package org.infinispan.server.hotrod;

import static java.lang.String.format;
import static org.infinispan.server.core.logging.Log.SERVER;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import org.infinispan.commons.TimeoutException;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.server.hotrod.logging.HotRodAccessLogging;
import org.infinispan.server.hotrod.logging.Log;
import org.infinispan.topology.MissingMembersException;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;

public class BaseRequestProcessor {
   private static final Log log = Log.getLog(BaseRequestProcessor.class);

   protected final Channel channel;
   protected final Executor executor;
   protected final HotRodServer server;
   private final HotRodAccessLogging accessLogging;

   BaseRequestProcessor(Channel channel, Executor executor, HotRodServer server) {
      this.channel = channel;
      this.executor = executor;
      this.server = server;
      this.accessLogging = server.accessLogging();
   }

   Channel channel() {
      return channel;
   }

   void writeException(HotRodHeader header, Throwable cause) {
      if (header == null) {
         writeException(null, 0, cause);
      } else {
         writeException(header, header.messageId, cause);
      }
   }

   void writeException(HotRodHeader header, long requestId, Throwable cause) {
      if (cause instanceof CompletionException && cause.getCause() != null) {
         cause = cause.getCause();
      }
      String msg = cause.toString();
      OperationStatus status;
      if (cause instanceof InvalidMagicIdException) {
         SERVER.exceptionReported(cause);
         status = OperationStatus.InvalidMagicOrMsgId;
      } else if (cause instanceof HotRodUnknownOperationException) {
         SERVER.exceptionReported(cause);
         HotRodUnknownOperationException hruoe = (HotRodUnknownOperationException) cause;
         header = hruoe.toHeader();
         status = OperationStatus.UnknownOperation;
      } else if (cause instanceof UnknownVersionException) {
         SERVER.exceptionReported(cause);
         UnknownVersionException uve = (UnknownVersionException) cause;
         header = uve.toHeader();
         status = OperationStatus.UnknownVersion;
      } else if (cause instanceof RequestParsingException) {
         if (cause instanceof CacheNotFoundException)
            log.debug(cause.getMessage());
         else
            SERVER.exceptionReported(cause);

         msg = cause.getCause() == null ? cause.toString() : format("%s: %s", cause.getMessage(), cause.getCause().toString());
         RequestParsingException rpe = (RequestParsingException) cause;
         header = rpe.toHeader();
         status = OperationStatus.ParseError;
      } else if (cause instanceof IOException) {
         status = OperationStatus.ParseError;
      } else if (cause instanceof TimeoutException) {
         status = OperationStatus.OperationTimedOut;
      } else if (cause instanceof IllegalStateException || isExceptionTrace(cause)) {
         if (isExceptionTrace(cause)) {
            log.trace("Exception reported", cause);
         } else {
            // Some internal server code could throw this, so make sure it's logged
            SERVER.exceptionReported(cause);
         }
         if (header != null) {
            status = header.encoder().errorStatus(cause);
            msg = createErrorMsg(cause);
         } else {
            status = OperationStatus.ServerError;
         }
      } else if (header != null) {
         if (cause instanceof MissingMembersException) log.warn(cause.getMessage());
         else SERVER.exceptionReported(cause);
         status = header.encoder().errorStatus(cause);
         msg = createErrorMsg(cause);
      } else {
         SERVER.exceptionReported(cause);
         status = OperationStatus.ServerError;
      }
      HotRodOperation op;
      if (header == null) {
         op = HotRodOperation.ERROR;
         header = new HotRodHeader(HotRodOperation.ERROR, (byte) 0, requestId, "", 0, (short) 1, 0, MediaType.MATCH_ALL, MediaType.MATCH_ALL, null);
      } else {
         op = header.op;
         header.op = HotRodOperation.ERROR;
      }
      ByteBuf buf = header.encoder().errorResponse(header, server, channel, msg, status);
      int responseBytes = buf.readableBytes();
      ChannelFuture future = channel.writeAndFlush(buf);
      if (header instanceof AccessLoggingHeader) {
         // Keep header with ERROR to flush to client, but revert to original operation when writing to access log.
         header.op = op;
         accessLogging.logException(future, (AccessLoggingHeader) header, cause.getMessage(), responseBytes);
      }
   }

   void writeSuccess(HotRodHeader header, CacheEntry<byte[], byte[]> entry) {
      if (header.hasFlag(ProtocolFlag.ForceReturnPreviousValue)) {
         writeResponse(header, header.encoder().successResponse(header, server, channel, entry));
      } else {
         writeResponse(header, header.encoder().emptyResponse(header, server, channel, OperationStatus.Success));
      }
   }

   void writeSuccess(HotRodHeader header) {
      writeResponse(header, header.encoder().emptyResponse(header, server, channel, OperationStatus.Success));
   }

   void writeNotExecuted(HotRodHeader header, CacheEntry<byte[], byte[]> prev) {
      if (header.hasFlag(ProtocolFlag.ForceReturnPreviousValue)) {
         writeResponse(header, header.encoder().notExecutedResponse(header, server, channel, prev));
      } else {
         writeResponse(header, header.encoder().emptyResponse(header, server, channel, OperationStatus.OperationNotExecuted));
      }
   }

   void writeNotExecuted(HotRodHeader header) {
      writeResponse(header, header.encoder().emptyResponse(header, server, channel, OperationStatus.OperationNotExecuted));
   }

   void writeNotExist(HotRodHeader header) {
      writeResponse(header, header.encoder().notExistResponse(header, server, channel));
   }

   protected void writeResponse(HotRodHeader header, ByteBuf buf) {
      int responseBytes = buf.readableBytes();
      ChannelPromise channelPromise;
      if (header instanceof AccessLoggingHeader) {
         channelPromise = channel.newPromise();
         accessLogging.logOK(channelPromise, (AccessLoggingHeader) header, responseBytes);
      } else {
         channelPromise = channel.voidPromise();
      }
      channel.writeAndFlush(buf, channelPromise);
   }

   private String createErrorMsg(Throwable t) {
      Set<Throwable> causes = new LinkedHashSet<>();
      Throwable initial = t;
      while (initial != null && !causes.contains(initial)) {
         causes.add(initial);
         initial = initial.getCause();
      }
      return causes.stream().map(Object::toString).collect(Collectors.joining("\n"));
   }

   private boolean isExceptionTrace(Throwable t) {
      return t instanceof MissingMembersException;
   }
}
