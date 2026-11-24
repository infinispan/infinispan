package org.infinispan.server.resp.commands.stream;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;

import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.exception.RespCommandException;
import org.infinispan.server.resp.serialization.JavaObjectSerializer;
import org.infinispan.server.resp.serialization.Resp3Type;
import org.infinispan.server.resp.serialization.ResponseWriter;
import org.infinispan.server.resp.stream.StreamEntry;
import org.infinispan.server.resp.stream.StreamID;

/**
 * Shared helpers for the stream commands: range bound and trimming spec parsing, and response serialization.
 *
 * @since 16.4
 */
final class StreamCommandUtils {

   private StreamCommandUtils() {
   }

   /**
    * The trimming specification for {@code XADD} and {@code XTRIM}:
    * {@code <MAXLEN | MINID> [= | ~] threshold [LIMIT count]}.
    *
    * <p>
    * The approximate trimming ({@code ~}) is implemented as an exact trimming, with the {@code LIMIT} option capping
    * the number of evicted entries.
    * </p>
    *
    * @param strategy One of {@link #TRIM_NONE}, {@link #TRIM_MAXLEN} or {@link #TRIM_MINID}.
    * @param maxlen The maximum length to keep with {@code MAXLEN}.
    * @param minId The threshold ID with {@code MINID}.
    * @param limit The maximum number of entries to evict, or a negative value for no limit.
    * @param nextPos The position of the first argument after the trimming specification.
    */
   record TrimSpec(int strategy, long maxlen, StreamID minId, long limit, int nextPos) {
   }

   static final int TRIM_NONE = 0;
   static final int TRIM_MAXLEN = 1;
   static final int TRIM_MINID = 2;

   private static final byte[] MAXLEN = "MAXLEN".getBytes(StandardCharsets.US_ASCII);
   private static final byte[] MINID = "MINID".getBytes(StandardCharsets.US_ASCII);
   private static final byte[] LIMIT = "LIMIT".getBytes(StandardCharsets.US_ASCII);

   /**
    * Parses the trimming specification {@code <MAXLEN | MINID> [= | ~] threshold [LIMIT count]} starting at the
    * provided position. The {@code LIMIT} option is only valid together with the approximate ({@code ~}) trimming,
    * and a {@code LIMIT} of zero disables the eviction limit, following the Redis semantics.
    *
    * @param arguments The command arguments.
    * @param pos The position of the trimming strategy token.
    * @return The parsed specification.
    * @throws RespCommandException When the specification is malformed.
    */
   static TrimSpec parseTrimSpec(List<byte[]> arguments, int pos) {
      boolean maxlenStrategy = matches(arguments.get(pos), MAXLEN);
      if (!maxlenStrategy && !matches(arguments.get(pos), MINID)) {
         throw new RespCommandException("ERR syntax error");
      }
      pos++;

      boolean approximate = false;
      if (pos < arguments.size() && (isToken(arguments.get(pos), '=') || isToken(arguments.get(pos), '~'))) {
         approximate = isToken(arguments.get(pos), '~');
         pos++;
      }

      if (pos >= arguments.size()) {
         throw new RespCommandException("ERR syntax error");
      }

      long maxlen = 0;
      StreamID minId = null;
      if (maxlenStrategy) {
         maxlen = ArgumentUtils.toLong(arguments.get(pos++));
         if (maxlen < 0) {
            throw new RespCommandException("ERR The MAXLEN argument must be >= 0");
         }
      } else {
         try {
            minId = StreamID.parse(arguments.get(pos++));
         } catch (NumberFormatException e) {
            throw new RespCommandException("ERR Invalid stream ID specified as stream command argument");
         }
      }

      long limit = -1;
      if (pos < arguments.size() && matches(arguments.get(pos), LIMIT)) {
         if (!approximate) {
            throw new RespCommandException("ERR syntax error, LIMIT cannot be used without the special ~ option");
         }
         pos++;
         if (pos >= arguments.size()) {
            throw new RespCommandException("ERR syntax error");
         }
         limit = ArgumentUtils.toLong(arguments.get(pos++));
         if (limit < 0) {
            throw new RespCommandException("ERR The LIMIT argument must be >= 0");
         }
         // Following the Redis semantics, LIMIT 0 disables the eviction limit instead of capping it at zero.
         if (limit == 0) {
            limit = -1;
         }
      }

      return new TrimSpec(maxlenStrategy ? TRIM_MAXLEN : TRIM_MINID, maxlen, minId, limit, pos);
   }

   /**
    * Case-insensitive comparison against an uppercase ASCII keyword. Unlike {@link RespUtil#isAsciiBytesEquals},
    * this comparison does not assert the compared argument contains only letters, as the stream commands compare
    * arbitrary user tokens, such as entry IDs, against the option keywords.
    */
   static boolean matches(byte[] argument, byte[] keyword) {
      if (argument.length != keyword.length) {
         return false;
      }
      for (int i = 0; i < keyword.length; i++) {
         byte b = argument[i];
         if (b >= 'a' && b <= 'z') {
            b -= 32;
         }
         if (b != keyword[i]) {
            return false;
         }
      }
      return true;
   }

   /**
    * Checks whether the argument is the provided single-character token.
    */
   static boolean isToken(byte[] argument, char token) {
      return argument.length == 1 && argument[0] == token;
   }

   /**
    * A range bound: the ID with its exclusivity. The special {@code -} and {@code +} tokens are resolved to
    * {@link StreamID#ZERO} and {@link StreamID#MAX} respectively.
    */
   record RangeBound(StreamID id, boolean exclusive) {
   }

   /**
    * Parses a range bound for {@code XRANGE} and {@code XREVRANGE}. Handles the special {@code -} and {@code +}
    * tokens, the incomplete {@code <ms>} format, and the exclusive {@code (} prefix. With the incomplete format, the
    * missing sequence part is zero for the start of the interval and the maximal unsigned value for its end.
    *
    * @param argument The bound to parse.
    * @param isStart Whether the bound is the start of the interval.
    * @return The parsed bound.
    * @throws IllegalArgumentException When the bound is malformed, or the exclusive prefix is utilized with an
    *         interval edge that cannot be honored.
    */
   static RangeBound parseRangeBound(byte[] argument, boolean isStart) {
      boolean exclusive = argument.length > 0 && argument[0] == '(';
      byte[] value = argument;
      if (exclusive) {
         if (argument.length == 1) {
            throw new RespCommandException("ERR Invalid stream ID specified as stream command argument");
         }
         value = new byte[argument.length - 1];
         System.arraycopy(argument, 1, value, 0, value.length);
      }

      StreamID id;
      if (!exclusive && isToken(value, '-')) {
         id = StreamID.ZERO;
      } else if (!exclusive && isToken(value, '+')) {
         id = StreamID.MAX;
      } else {
         try {
            // An incomplete <ms> bound has a zero sequence part as the start of the interval, and the maximal
            // unsigned sequence part as its end, following the Redis range semantics.
            id = StreamID.parse(value, isStart ? 0 : -1L);
         } catch (NumberFormatException e) {
            throw new RespCommandException("ERR Invalid stream ID specified as stream command argument");
         }
      }

      // An exclusive start bound must have a successor, and an exclusive end bound must have a predecessor.
      if (exclusive) {
         if (isStart && id.compareTo(StreamID.MAX) == 0) {
            throw new RespCommandException("ERR invalid start ID for the interval");
         }
         if (!isStart && id.isZero()) {
            throw new RespCommandException("ERR invalid end ID for the interval");
         }
      }
      return new RangeBound(id, exclusive);
   }

   /**
    * Serializes a single stream entry in the RESP array format: a two element array with the ID and the flat list of
    * field-value pairs.
    */
   static final JavaObjectSerializer<StreamEntry> ENTRY_SERIALIZER = new JavaObjectSerializer<>() {
      @Override
      public void accept(StreamEntry entry, ResponseWriter writer) {
         writer.array(List.of(entry.id().asBytes(), entry.fields()), (Object o, ResponseWriter w) -> {
            if (o instanceof byte[] bytes) {
               w.string(bytes);
            } else {
               //noinspection unchecked
               w.array((Collection<byte[]>) o, Resp3Type.BULK_STRING);
            }
         });
      }
   };

   /**
    * Serializes a list of stream entries as an array response.
    */
   static void writeEntries(List<StreamEntry> entries, ResponseWriter writer) {
      writer.array(entries, ENTRY_SERIALIZER);
   }
}
