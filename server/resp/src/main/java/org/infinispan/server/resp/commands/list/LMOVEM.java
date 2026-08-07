package org.infinispan.server.resp.commands.list;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.multimap.impl.EmbeddedMultimapListCache;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.RespUtil;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.logging.Log;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * LMOVEM
 *
 * @see <a href="https://redis.io/commands/lmovem/">LMOVEM</a>
 * @since 16.3
 */
public class LMOVEM extends RespCommand implements Resp3Command {
   private static final byte[] LEFT = "LEFT".getBytes(StandardCharsets.US_ASCII);
   private static final byte[] RIGHT = "RIGHT".getBytes(StandardCharsets.US_ASCII);
   private static final byte[] COUNT = "COUNT".getBytes(StandardCharsets.US_ASCII);
   private static final byte[] EXACTLY = "EXACTLY".getBytes(StandardCharsets.US_ASCII);
   private static final byte[] OBO = "OBO".getBytes(StandardCharsets.US_ASCII);
   private static final byte[] BULK = "BULK".getBytes(StandardCharsets.US_ASCII);

   public LMOVEM() {
      super(-5, 1, 2, 1, AclCategory.WRITE.mask() | AclCategory.LIST.mask() | AclCategory.SLOW.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      LmovemConfig config = parseLmovemArgs(handler, arguments, 0);
      if (config == null) return handler.myStage();

      CompletionStage<Collection<byte[]>> cs = executeLmovem(handler.getListMultimap(), config);
      return handler.stageToReturn(cs, ctx, ResponseWriter.ARRAY_BULK_STRING);
   }

   /**
    * Parse LMOVEM-specific arguments starting at the given offset.
    * Arguments: source destination LEFT|RIGHT LEFT|RIGHT [COUNT|EXACTLY count OBO|BULK]
    *
    * @param handler the handler for error responses
    * @param arguments the argument list
    * @param offset index of the source argument
    * @return parsed config, or null if an error response was written
    */
   public static LmovemConfig parseLmovemArgs(Resp3Handler handler, List<byte[]> arguments, int offset) {
      if (arguments.size() - offset != 4 && arguments.size() - offset != 7) {
         handler.writer().syntaxError();
         return null;
      }

      byte[] source = arguments.get(offset);
      byte[] destination = arguments.get(offset + 1);

      byte[] srcDir = arguments.get(offset + 2);
      byte[] dstDir = arguments.get(offset + 3);

      boolean sourceLeft;
      if (RespUtil.isAsciiBytesEquals(LEFT, srcDir)) {
         sourceLeft = true;
      } else if (RespUtil.isAsciiBytesEquals(RIGHT, srcDir)) {
         sourceLeft = false;
      } else {
         handler.writer().syntaxError();
         return null;
      }

      boolean destLeft;
      if (RespUtil.isAsciiBytesEquals(LEFT, dstDir)) {
         destLeft = true;
      } else if (RespUtil.isAsciiBytesEquals(RIGHT, dstDir)) {
         destLeft = false;
      } else {
         handler.writer().syntaxError();
         return null;
      }

      int count = 1;
      boolean exactly = false;
      boolean obo = true;

      if (arguments.size() - offset == 7) {
         byte[] mode = arguments.get(offset + 4);
         if (RespUtil.isAsciiBytesEquals(COUNT, mode)) {
            exactly = false;
         } else if (RespUtil.isAsciiBytesEquals(EXACTLY, mode)) {
            exactly = true;
         } else {
            handler.writer().syntaxError();
            return null;
         }

         try {
            count = ArgumentUtils.toInt(arguments.get(offset + 5));
         } catch (NumberFormatException e) {
            handler.writer().syntaxError();
            return null;
         }

         if (count <= 0) {
            handler.writer().mustBePositive("count");
            return null;
         }

         byte[] ordering = arguments.get(offset + 6);
         if (RespUtil.isAsciiBytesEquals(OBO, ordering)) {
            obo = true;
         } else if (RespUtil.isAsciiBytesEquals(BULK, ordering)) {
            obo = false;
         } else {
            handler.writer().syntaxError();
            return null;
         }
      }

      return new LmovemConfig(source, destination, sourceLeft, destLeft, count, exactly, obo);
   }

   /**
    * Execute the LMOVEM operation: poll from source, push to destination.
    *
    * @return a stage that completes with the moved elements, or null if nothing was moved
    */
   public static CompletionStage<Collection<byte[]>> executeLmovem(
         EmbeddedMultimapListCache<byte[], byte[]> listMultimap, LmovemConfig config) {

      boolean sameList = Arrays.equals(config.source, config.destination);
      if (!sameList) {
         Log.SERVER.lmoveConsistencyMessage();
      }

      // Same-list, count=1 optimization (like LMOVE)
      if (sameList && config.count == 1) {
         CompletionStage<byte[]> singleResult;
         if (config.sourceLeft && config.destLeft) {
            singleResult = listMultimap.index(config.source, 0);
         } else if (!config.sourceLeft && !config.destLeft) {
            singleResult = listMultimap.index(config.source, -1);
         } else {
            singleResult = listMultimap.rotate(config.source, config.sourceLeft);
         }
         return singleResult.thenApply(element -> element == null ? null : List.of(element));
      }

      CompletionStage<Collection<byte[]>> pollStage;
      if (config.exactly) {
         // EXACTLY mode: check size first, return null if insufficient
         pollStage = listMultimap.size(config.source).thenCompose(size -> {
            if (size < config.count) return CompletableFutures.completedNull();
            return config.sourceLeft
                  ? listMultimap.pollFirst(config.source, config.count)
                  : listMultimap.pollLast(config.source, config.count);
         });
      } else {
         pollStage = config.sourceLeft
               ? listMultimap.pollFirst(config.source, config.count)
               : listMultimap.pollLast(config.source, config.count);
      }

      return pollStage.thenCompose(polled -> {
         if (polled == null || polled.isEmpty()) return CompletableFutures.completedNull();

         List<byte[]> elements = new ArrayList<>(polled);

         // BULK + RIGHT pop: reverse to restore source order
         if (!config.obo && !config.sourceLeft) {
            Collections.reverse(elements);
         }

         CompletionStage<Void> offerStage;
         if (config.destLeft) {
            if (config.obo) {
               offerStage = listMultimap.offerFirst(config.destination, elements);
            } else {
               // BULK LEFT: reverse for offerFirst so destination preserves source order
               List<byte[]> reversed = new ArrayList<>(elements);
               Collections.reverse(reversed);
               offerStage = listMultimap.offerFirst(config.destination, reversed);
            }
         } else {
            offerStage = listMultimap.offerLast(config.destination, elements);
         }

         return offerStage.thenApply(v -> {
            if (config.obo && config.destLeft) {
               // OBO LEFT: offerFirst reversed the order, return destination order
               List<byte[]> result = new ArrayList<>(elements);
               Collections.reverse(result);
               return (Collection<byte[]>) result;
            }
            return (Collection<byte[]>) elements;
         });
      });
   }

   public static class LmovemConfig {
      public final byte[] source;
      public final byte[] destination;
      public final boolean sourceLeft;
      public final boolean destLeft;
      public final int count;
      public final boolean exactly;
      public final boolean obo;

      public LmovemConfig(byte[] source, byte[] destination, boolean sourceLeft, boolean destLeft,
                           int count, boolean exactly, boolean obo) {
         this.source = source;
         this.destination = destination;
         this.sourceLeft = sourceLeft;
         this.destLeft = destLeft;
         this.count = count;
         this.exactly = exactly;
         this.obo = obo;
      }
   }
}
