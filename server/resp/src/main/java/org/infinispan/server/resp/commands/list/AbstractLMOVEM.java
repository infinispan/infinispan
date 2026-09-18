package org.infinispan.server.resp.commands.list;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.multimap.impl.EmbeddedMultimapListCache;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespUtil;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.logging.Log;

/**
 * Shared logic for the {@code LMOVEM} and {@code BLMOVEM} commands: argument parsing and the
 * underlying poll-from-source/push-to-destination operation.
 */
public abstract class AbstractLMOVEM extends RespCommand {
   protected static final byte[] LEFT = "LEFT".getBytes(StandardCharsets.US_ASCII);
   protected static final byte[] RIGHT = "RIGHT".getBytes(StandardCharsets.US_ASCII);
   protected static final byte[] COUNT = "COUNT".getBytes(StandardCharsets.US_ASCII);
   protected static final byte[] EXACTLY = "EXACTLY".getBytes(StandardCharsets.US_ASCII);
   protected static final byte[] OBO = "OBO".getBytes(StandardCharsets.US_ASCII);
   protected static final byte[] BULK = "BULK".getBytes(StandardCharsets.US_ASCII);

   protected AbstractLMOVEM(int arity, long aclMask) {
      super(arity, 1, 2, 1, aclMask);
   }

    /**
     * Parse the shared LMOVEM arguments.
     * Arguments: source destination LEFT|RIGHT LEFT|RIGHT [timeout] [COUNT|EXACTLY count OBO|BULK]
     *
     * @param handler the handler for error responses
     * @param arguments the argument list
     * @param withTimeout whether a timeout argument is present between the directions and the optional block
     * @return parsed config, or null if an error response was written
     */
    protected static LmovemConfig parseArguments(Resp3Handler handler, List<byte[]> arguments, boolean withTimeout) {
      int size = arguments.size();
      int baseArgs = withTimeout ? 5 : 4;
      if (size != baseArgs && size != baseArgs + 3) {
         handler.writer().syntaxError();
         return null;
      }

      byte[] source = arguments.get(0);
      byte[] destination = arguments.get(1);

      Boolean sourceLeft = parseDirection(handler, arguments.get(2));
      if (sourceLeft == null) return null;
      Boolean destLeft = parseDirection(handler, arguments.get(3));
      if (destLeft == null) return null;

      long timeout = 0;
      if (withTimeout) {
         long timeoutSeconds = ArgumentUtils.toLong(arguments.get(4));
         if (timeoutSeconds < 0) {
            handler.writer().mustBePositive();
            return null;
         }
         timeout = Duration.of(timeoutSeconds, ChronoUnit.SECONDS).toMillis();
      }

      int count = 1;
      boolean exactly = false;
      boolean obo = true;

      if (size == baseArgs + 3) {
         byte[] mode = arguments.get(baseArgs);
         if (RespUtil.isAsciiBytesEquals(COUNT, mode)) {
            exactly = false;
         } else if (RespUtil.isAsciiBytesEquals(EXACTLY, mode)) {
            exactly = true;
         } else {
            handler.writer().syntaxError();
            return null;
         }

         try {
            count = ArgumentUtils.toInt(arguments.get(baseArgs + 1));
         } catch (NumberFormatException e) {
            handler.writer().syntaxError();
            return null;
         }

         if (count <= 0) {
            handler.writer().mustBePositive("count");
            return null;
         }

         byte[] ordering = arguments.get(baseArgs + 2);
         if (RespUtil.isAsciiBytesEquals(OBO, ordering)) {
            obo = true;
         } else if (RespUtil.isAsciiBytesEquals(BULK, ordering)) {
            obo = false;
         } else {
            handler.writer().syntaxError();
            return null;
         }
      }

      return new LmovemConfig(source, destination, sourceLeft, destLeft, count, exactly, obo, timeout);
   }

   private static Boolean parseDirection(Resp3Handler handler, byte[] direction) {
      if (RespUtil.isAsciiBytesEquals(LEFT, direction)) return true;
      if (RespUtil.isAsciiBytesEquals(RIGHT, direction)) return false;
      handler.writer().syntaxError();
      return null;
   }

   /**
    * Execute the LMOVEM operation: poll from source, push to destination.
    *
    * @return a stage that completes with the moved elements, or null if nothing was moved
    */
   public static CompletionStage<Collection<byte[]>> executeLmovem(
         String command, EmbeddedMultimapListCache<byte[], byte[]> listMultimap, LmovemConfig config) {

      boolean sameList = Arrays.equals(config.source(), config.destination());
      if (!sameList) {
         Log.SERVER.lmoveConsistencyMessage(command);
      }

      // Same-list, count=1 optimization (like LMOVE)
      if (sameList && config.count() == 1) {
         CompletionStage<byte[]> singleResult;
         if (config.sourceLeft() && config.destLeft()) {
            singleResult = listMultimap.index(config.source(), 0);
         } else if (!config.sourceLeft() && !config.destLeft()) {
            singleResult = listMultimap.index(config.source(), -1);
         } else {
            singleResult = listMultimap.rotate(config.source(), config.sourceLeft());
         }
         return singleResult.thenApply(element -> element == null ? null : List.of(element));
      }

      // For different lists, validate the destination type before polling from the source so that
      // a wrong-type destination does not leave the source partially modified.
      CompletionStage<?> precheck = sameList
            ? CompletableFutures.completedNull()
            : listMultimap.get(config.destination());

      return precheck.thenCompose(ignore -> pollThenOffer(listMultimap, config));
   }

   private static CompletionStage<Collection<byte[]>> pollThenOffer(
         EmbeddedMultimapListCache<byte[], byte[]> listMultimap, LmovemConfig config) {

      CompletionStage<Collection<byte[]>> pollStage;
      if (config.exactly()) {
         // EXACTLY mode: check size first, return null if insufficient
         pollStage = listMultimap.size(config.source()).thenCompose(size -> {
            if (size < config.count()) return CompletableFutures.completedNull();
            return config.sourceLeft()
                  ? listMultimap.pollFirst(config.source(), config.count())
                  : listMultimap.pollLast(config.source(), config.count());
         });
      } else {
         pollStage = config.sourceLeft()
               ? listMultimap.pollFirst(config.source(), config.count())
               : listMultimap.pollLast(config.source(), config.count());
      }

      return pollStage.thenCompose(polled -> {
         if (polled == null || polled.isEmpty()) return CompletableFutures.completedNull();

         List<byte[]> elements = new ArrayList<>(polled);

         // BULK + RIGHT pop: reverse to restore source order
         if (!config.obo() && !config.sourceLeft()) {
            Collections.reverse(elements);
         }

         CompletionStage<Void> offerStage;
         if (config.destLeft()) {
            if (config.obo()) {
               offerStage = listMultimap.offerFirst(config.destination(), elements);
            } else {
               // BULK LEFT: reverse for offerFirst so destination preserves source order
               List<byte[]> reversed = new ArrayList<>(elements);
               Collections.reverse(reversed);
               offerStage = listMultimap.offerFirst(config.destination(), reversed);
            }
         } else {
            offerStage = listMultimap.offerLast(config.destination(), elements);
         }

         return offerStage.thenApply(v -> {
            if (config.obo() && config.destLeft()) {
               // OBO LEFT: offerFirst reversed the order, return destination order
               List<byte[]> result = new ArrayList<>(elements);
               Collections.reverse(result);
               return result;
            }
            return elements;
         });
      });
   }

   public record LmovemConfig(byte[] source, byte[] destination, boolean sourceLeft, boolean destLeft, int count,
                              boolean exactly, boolean obo, long timeout) {
   }
}
