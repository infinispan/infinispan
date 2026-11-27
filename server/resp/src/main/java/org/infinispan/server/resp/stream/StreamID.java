package org.infinispan.server.resp.stream;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Represents a Redis stream entry ID, composed of two unsigned 64-bit integers: {@code <milliseconds>-<sequence>}.
 *
 * <p>
 * The unsigned semantics are implemented with {@link Long#compareUnsigned(long, long)} and
 * {@link Long#toUnsignedString(long)}, as Redis allows values up to {@code 18446744073709551615} for both parts.
 * </p>
 *
 * @since 16.4
 */
@ProtoTypeId(ProtoStreamTypeIds.RESP_STREAM_ID)
public class StreamID implements Comparable<StreamID>, Serializable {

   /**
    * The smallest possible ID, {@code 0-0}. Not a valid entry ID, but valid as a range bound.
    */
   public static final StreamID ZERO = new StreamID(0, 0);

   /**
    * The largest possible ID, {@code 18446744073709551615-18446744073709551615}.
    */
   public static final StreamID MAX = new StreamID(-1L, -1L);

   private final long ms;
   private final long seq;

   @ProtoFactory
   public StreamID(long ms, long seq) {
      this.ms = ms;
      this.seq = seq;
   }

   @ProtoField(number = 1, defaultValue = "0")
   public long ms() {
      return ms;
   }

   @ProtoField(number = 2, defaultValue = "0")
   public long seq() {
      return seq;
   }

   /**
    * Parses a stream ID in the {@code <ms>-<seq>} or incomplete {@code <ms>} format. The incomplete format assumes a
    * zero sequence part. Special tokens ({@code -}, {@code +}, {@code $}, {@code *}) must be handled by the caller.
    *
    * @param argument The ID to parse.
    * @return The parsed ID, never {@code null}.
    * @throws NumberFormatException If the ID is malformed or its parts overflow an unsigned 64-bit integer.
    */
   public static StreamID parse(byte[] argument) {
      return parse(argument, 0);
   }

   /**
    * Parses a stream ID in the {@code <ms>-<seq>} or incomplete {@code <ms>} format, utilizing the provided sequence
    * part when the format is incomplete. The sequence part is interpreted as unsigned, so {@code -1} means the
    * maximal value {@code 18446744073709551615}. Special tokens ({@code -}, {@code +}, {@code $}, {@code *}) must be
    * handled by the caller.
    *
    * @param argument The ID to parse.
    * @param incompleteSequence The sequence part to utilize with the incomplete {@code <ms>} format, as an unsigned
    *        value.
    * @return The parsed ID, never {@code null}.
    * @throws NumberFormatException If the ID is malformed or its parts overflow an unsigned 64-bit integer.
    */
   public static StreamID parse(byte[] argument, long incompleteSequence) {
      String value = new String(argument, StandardCharsets.US_ASCII);
      int dash = value.indexOf('-');
      try {
         if (dash < 0) {
            return new StreamID(Long.parseUnsignedLong(value), incompleteSequence);
         }
         if (dash == 0 || dash == value.length() - 1) {
            throw new NumberFormatException("Invalid stream ID: " + value);
         }
         return new StreamID(Long.parseUnsignedLong(value.substring(0, dash)),
               Long.parseUnsignedLong(value.substring(dash + 1)));
      } catch (NumberFormatException e) {
         throw new NumberFormatException("Invalid stream ID: " + value);
      }
   }

   /**
    * @return The ID immediately succeeding this one, incrementing the sequence part. Overflow of the sequence part
    *         rolls over to the next millisecond.
    */
   public StreamID next() {
      long nextSeq = seq + 1;
      if (nextSeq == 0) {
         // Sequence part overflowed, move to the next millisecond.
         return new StreamID(ms + 1, 0);
      }
      return new StreamID(ms, nextSeq);
   }

   /**
    * @return {@code true} if this ID is equal to {@code 0-0}.
    */
   public boolean isZero() {
      return ms == 0 && seq == 0;
   }

   @Override
   public int compareTo(StreamID other) {
      int compare = Long.compareUnsigned(ms, other.ms);
      if (compare != 0) {
         return compare;
      }
      return Long.compareUnsigned(seq, other.seq);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      StreamID streamID = (StreamID) o;
      return ms == streamID.ms && seq == streamID.seq;
   }

   @Override
   public int hashCode() {
      return 31 * Long.hashCode(ms) + Long.hashCode(seq);
   }

   @Override
   public String toString() {
      return Long.toUnsignedString(ms) + "-" + Long.toUnsignedString(seq);
   }

   /**
    * @return The ID serialized in the {@code <ms>-<seq>} format.
    */
   public byte[] asBytes() {
      return toString().getBytes(StandardCharsets.US_ASCII);
   }
}
