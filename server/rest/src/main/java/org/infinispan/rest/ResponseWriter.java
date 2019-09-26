package org.infinispan.rest;

import static io.netty.handler.codec.http.HttpHeaderNames.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaderValues.KEEP_ALIVE;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;

import org.infinispan.rest.logging.RestAccessLoggingHandler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpChunkedInput;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.stream.ChunkedFile;
import io.netty.handler.stream.ChunkedStream;

/**
 * @since 10.0
 */
public enum ResponseWriter {

   EMPTY {
      @Override
      void writeResponse(ChannelHandlerContext ctx, FullHttpRequest request, NettyRestResponse response, boolean keepAlive) {
         HttpResponse res = response.getResponse();
         HttpUtil.setContentLength(res, 0);
         accessLog.log(ctx, request, response.getResponse());
         handleKeepAlive(res, ctx.writeAndFlush(response.getResponse()), keepAlive);
      }
   },
   FULL {
      @Override
      void writeResponse(ChannelHandlerContext ctx, FullHttpRequest request, NettyRestResponse response, boolean keepAlive) {
         HttpResponse res = response.getResponse();
         ByteBuf responseContent = ((FullHttpResponse) res).content();
         Object entity = response.getEntity();
         if (entity instanceof String) {
            ByteBufUtil.writeUtf8(responseContent, entity.toString());
         } else if (entity instanceof byte[]) {
            responseContent.writeBytes((byte[]) entity);
         }
         HttpUtil.setContentLength(res, responseContent.readableBytes());
         accessLog.log(ctx, request, response.getResponse());
         handleKeepAlive(res, ctx.writeAndFlush(res), keepAlive);
      }
   },
   CHUNKED_FILE {
      @Override
      void writeResponse(ChannelHandlerContext ctx, FullHttpRequest request, NettyRestResponse response, boolean keepAlive) {
         try {
            HttpResponse res = response.getResponse();
            File file = (File) response.getEntity();
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
            HttpUtil.setContentLength(res, randomAccessFile.length());
            accessLog.log(ctx, request, response.getResponse());
            ctx.write(res);
            ctx.writeAndFlush(new HttpChunkedInput(new ChunkedFile(randomAccessFile, 0, randomAccessFile.length(), 8192)), ctx.newProgressivePromise());
         } catch (IOException e) {
            throw new RestResponseException(e);
         }
      }
   },
   CHUNKED_STREAM {
      @Override
      void writeResponse(ChannelHandlerContext ctx, FullHttpRequest request, NettyRestResponse response, boolean keepAlive) {
         HttpResponse res = response.getResponse();
         res.headers().set(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);
         res.headers().set(CONNECTION, KEEP_ALIVE);
         InputStream inputStream = (InputStream) response.getEntity();
         accessLog.log(ctx, request, response.getResponse());
         ctx.write(res);
         ctx.writeAndFlush(new HttpChunkedInput(new ChunkedStream(inputStream)), ctx.newProgressivePromise());
      }
   };

   void handleKeepAlive(HttpResponse response, ChannelFuture future, boolean keepAlive) {
      if (!keepAlive) {
         response.headers().set(CONNECTION, HttpHeaderValues.CLOSE);
         future.addListener(ChannelFutureListener.CLOSE);
      }
   }

   final RestAccessLoggingHandler accessLog = new RestAccessLoggingHandler();

   abstract void writeResponse(ChannelHandlerContext ctx, FullHttpRequest request, NettyRestResponse response, boolean keepAlive);

   static ResponseWriter forContent(Object content) {
      if (content == null) return EMPTY;
      if (content instanceof String || content instanceof byte[]) return FULL;
      if (content instanceof File) return CHUNKED_FILE;
      if (content instanceof InputStream) return CHUNKED_STREAM;
      throw new RestResponseException(INTERNAL_SERVER_ERROR, "Cannot write content of type " + content.getClass());
   }
}
