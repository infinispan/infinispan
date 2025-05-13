package org.infinispan.rest;

import static io.netty.handler.codec.http.HttpHeaderNames.CACHE_CONTROL;
import static io.netty.handler.codec.http.HttpHeaderNames.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaderValues.KEEP_ALIVE;
import static io.netty.handler.codec.http.HttpHeaderValues.NO_CACHE;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.infinispan.rest.logging.Log;
import org.infinispan.rest.logging.RestAccessLoggingHandler;
import org.infinispan.rest.stream.CacheChunkedStream;
import org.infinispan.util.logging.LogFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpChunkedInput;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.stream.ChunkedFile;

/**
 * @since 10.0
 */
public enum ResponseWriter {

   EMPTY {
      @Override
      void writeResponse(ChannelHandlerContext ctx, FullHttpRequest request, NettyRestResponse response) {
         HttpResponse res = response.getResponse();
         HttpUtil.setContentLength(res, 0);
         log(ctx, request, res);
         ctx.writeAndFlush(response.getResponse());
      }
   },
   FULL {
      @Override
      void writeResponse(ChannelHandlerContext ctx, FullHttpRequest request, NettyRestResponse response) {
         HttpResponse res = response.getResponse();
         ByteBuf responseContent = ((FullHttpResponse) res).content();
         Object entity = response.getEntity();
         if (entity instanceof byte[]) {
            responseContent.writeBytes((byte[]) entity);
         } else if (entity instanceof ByteArrayOutputStream) {
            responseContent.writeBytes(((ByteArrayOutputStream)entity).toByteArray());
         } else {
            ByteBufUtil.writeUtf8(responseContent, entity.toString());
         }
         HttpUtil.setContentLength(res, responseContent.readableBytes());
         log(ctx, request, res);
         ctx.writeAndFlush(res);
      }
   },
   CHUNKED_FILE {
      @Override
      void writeResponse(ChannelHandlerContext ctx, FullHttpRequest request, NettyRestResponse response) {
         try {
            // The file is closed by the ChunkedWriteHandler
            RandomAccessFile randomAccessFile = new RandomAccessFile((File) response.getEntity(), "r");
            HttpResponse res = response.getResponse();
            log(ctx, request, res);
            res.headers().add(ResponseHeader.TRANSFER_ENCODING.getValue(), HttpHeaderValues.CHUNKED);
            res.headers().remove(ResponseHeader.CONTENT_LENGTH_HEADER.getValue());
            ctx.write(res);
            ctx.writeAndFlush(new HttpChunkedInput(new ChunkedFile(randomAccessFile, 0, randomAccessFile.length(), 8192)), ctx.newProgressivePromise());
         } catch (IOException e) {
            throw new RestResponseException(e);
         }
      }
   },
   CHUNKED_STREAM {
      @Override
      void writeResponse(ChannelHandlerContext ctx, FullHttpRequest request, NettyRestResponse response) {
         HttpResponse res = response.getResponse();
         res.headers().set(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);
         res.headers().remove(ResponseHeader.CONTENT_LENGTH_HEADER.getValue());
         res.headers().set(CONNECTION, KEEP_ALIVE);
         log(ctx, request, res);
         ctx.write(res);
         CacheChunkedStream<?> chunked = (CacheChunkedStream<?>) response.getEntity();
         chunked.subscribe(ctx);
      }
   },
   EVENT_STREAM {
      @Override
      void writeResponse(ChannelHandlerContext ctx, FullHttpRequest request, NettyRestResponse response) {
         HttpResponse res = response.getResponse();
         res.headers().set(CACHE_CONTROL, NO_CACHE);
         res.headers().set(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);
         res.headers().remove(ResponseHeader.CONTENT_LENGTH_HEADER.getValue());
         res.headers().set(CONNECTION, KEEP_ALIVE);
         log(ctx, request, res);
         ctx.writeAndFlush(res).addListener(v -> {
            EventStream eventStream = (EventStream) response.getEntity();
            eventStream.setChannelHandlerContext(ctx);
         });
      }
   };

   void log(ChannelHandlerContext ctx,  FullHttpRequest req, HttpResponse rsp) {
      accessLog.log(ctx, req, rsp);
      if (logger.isTraceEnabled()) {
         logger.trace(HttpMessageUtil.dumpResponse(rsp));
      }
   }

   static final Log logger = LogFactory.getLog(ResponseWriter.class, Log.class);
   final RestAccessLoggingHandler accessLog = new RestAccessLoggingHandler();

   abstract void writeResponse(ChannelHandlerContext ctx, FullHttpRequest request, NettyRestResponse response);

   static ResponseWriter forContent(FullHttpRequest request, Object content) {
      if (content == null || HttpMethod.HEAD.equals(request.method())) return EMPTY;
      if (content instanceof File) return CHUNKED_FILE;
      if (content instanceof CacheChunkedStream) return CHUNKED_STREAM;
      if (content instanceof EventStream) return EVENT_STREAM;
      return FULL;
   }
}
