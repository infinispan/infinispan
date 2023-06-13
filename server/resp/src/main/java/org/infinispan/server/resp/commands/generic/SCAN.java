package org.infinispan.server.resp.commands.generic;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.reactive.publisher.impl.DeliveryGuarantee;
import org.infinispan.server.iteration.DefaultIterationManager;
import org.infinispan.server.iteration.IterableIterationResult;
import org.infinispan.server.iteration.IterationState;
import org.infinispan.server.resp.ByteBufferUtils;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespErrorUtil;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.RespTypes;
import org.infinispan.server.resp.Util;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.filter.GlobMatchFilterConverterFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;

import static org.infinispan.server.resp.RespConstants.CRLF_STRING;

/**
 * <a href="https://redis.io/commands/scan/">SCAN</a>
 *
 * @since 15.0
 */
public class SCAN extends RespCommand implements Resp3Command {
   private static final int DEFAULT_COUNT = 10;
   private static final byte[] COUNT = "COUNT".getBytes(StandardCharsets.US_ASCII);
   private static final byte[] MATCH = "MATCH".getBytes(StandardCharsets.US_ASCII);
   private static final byte[] TYPE = "TYPE".getBytes(StandardCharsets.US_ASCII);

   public SCAN() {
      super(-2, 0, 0, 0);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      int argc = arguments.size();
      String filterConverterFactory = null;
      List<byte[]> filterConverterParams = null;
      int count = -1;
      RespTypes type = null;
      if (argc > 1) {
         for (int i = 1; i < argc; i++) {
            byte[] arg = arguments.get(i);
            if (Util.isAsciiBytesEquals(MATCH, arg)) {
               if (++i >= argc) {
                  RespErrorUtil.syntaxError(handler.allocator());
                  return handler.myStage();
               } else {
                  filterConverterFactory = GlobMatchFilterConverterFactory.class.getName();
                  filterConverterParams = Collections.singletonList(arguments.get(i));
               }
            } else if (Util.isAsciiBytesEquals(COUNT, arg)) {
               if (++i >= argc) {
                  RespErrorUtil.syntaxError(handler.allocator());
                  return handler.myStage();
               } else {
                  try {
                     count = ArgumentUtils.toInt(arguments.get(i));
                  } catch (NumberFormatException e) {
                     RespErrorUtil.valueNotInteger(handler.allocator());
                     return handler.myStage();
                  }
               }
            } else if (Util.isAsciiBytesEquals(TYPE, arg)) {
               if (++i >= argc) {
                  RespErrorUtil.syntaxError(handler.allocator());
                  return handler.myStage();
               } else {
                  try {
                     type = RespTypes.valueOf(new String(arguments.get(i), StandardCharsets.US_ASCII));
                  } catch (IllegalArgumentException e) {
                     type = RespTypes.unknown;
                  }
               }
            }
         }
      }

      DefaultIterationManager iterationManager = handler.respServer().getIterationManager();
      String cursor = new String(arguments.get(0), StandardCharsets.US_ASCII);
      if ("0".equals(cursor)) {
         // New iterator
         if (count < 0) {
            count = DEFAULT_COUNT;
         }
         IterationState iterationState = iterationManager.start(handler.cache(), null, filterConverterFactory, filterConverterParams, MediaType.APPLICATION_OCTET_STREAM, count, false, DeliveryGuarantee.AT_LEAST_ONCE);
         iterationState.getReaper().registerChannel(ctx.channel());
         cursor = iterationState.getId();
      }
      IterableIterationResult result = iterationManager.next(cursor, count);
      IterableIterationResult.Status status = result.getStatusCode();
      if (status == IterableIterationResult.Status.InvalidIteration) {
         // Let's just return 0
         ByteBufferUtils.stringToByteBuf("*2\r\n$1\r\n0\r\n*0\r\n", handler.allocator());
      } else {
         StringBuilder response = new StringBuilder();
         response.append("*2\r\n");
         int size = result.getEntries().size();
         if (status == IterableIterationResult.Status.Finished) {
            // We've reached the end of iteration, return a 0 cursor
            response.append("$1\r\n0\r\n");
            iterationManager.close(cursor);
         } else {
            response.append('$');
            response.append(cursor.length());
            response.append(CRLF_STRING);
            response.append(cursor);
            response.append(CRLF_STRING);
         }
         ByteBufferUtils.stringToByteBuf(response, handler.allocator());
         List<byte[]> keys = new ArrayList<>(size);
         for (CacheEntry<?, ?> entry : result.getEntries()) {
            keys.add((byte[]) entry.getKey());
         }
         ByteBufferUtils.bytesToResult(keys, handler.allocator());
      }
      return handler.myStage();
   }
}
