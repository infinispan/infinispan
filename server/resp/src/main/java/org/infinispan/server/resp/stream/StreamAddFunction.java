package org.infinispan.server.resp.stream;

import java.util.List;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.server.resp.exception.RespCommandException;
import org.infinispan.util.function.SerializableFunction;

/**
 * Function implementing the {@code XADD} semantics: append an entry to the stream, generating the entry ID when
 * requested, and applying the trimming strategy.
 *
 * <p>
 * The auto-generated ID utilizes the wall-clock time provided by the originator of the command, combined with the
 * stream's last ID: the ID is strictly monotonic even with concurrent {@code XADD} commands or clock skew. The
 * function must be deterministic, as it executes on all the owners of the key, which may have unsynchronized clocks:
 * the originator's clock travels with the command.
 * </p>
 *
 * @since 16.4
 */
@ProtoTypeId(ProtoStreamTypeIds.RESP_STREAM_ADD_FUNCTION)
public class StreamAddFunction implements SerializableFunction<EntryView.ReadWriteEntryView<byte[], StreamBucket>, byte[]> {

   /**
    * No trimming strategy.
    */
   public static final int TRIM_NONE = 0;

   /**
    * Trim the stream to a maximum length.
    */
   public static final int TRIM_MAXLEN = 1;

   /**
    * Trim the stream evicting entries with an ID lower than a threshold.
    */
   public static final int TRIM_MINID = 2;

   @ProtoField(1)
   final boolean nomkstream;

   @ProtoField(2)
   final int trimStrategy;

   @ProtoField(3)
   final long maxlen;

   @ProtoField(4)
   final StreamID minId;

   @ProtoField(5)
   final long limit;

   @ProtoField(6)
   final boolean autoId;

   @ProtoField(7)
   final boolean autoSeq;

   @ProtoField(8)
   final long idMs;

   @ProtoField(9)
   final long idSeq;

   @ProtoField(10)
   final List<byte[]> fields;

   @ProtoField(11)
   final long clockMs;

   @ProtoFactory
   public StreamAddFunction(boolean nomkstream, int trimStrategy, long maxlen, StreamID minId, long limit,
         boolean autoId, boolean autoSeq, long idMs, long idSeq, List<byte[]> fields, long clockMs) {
      this.nomkstream = nomkstream;
      this.trimStrategy = trimStrategy;
      this.maxlen = maxlen;
      this.minId = minId;
      this.limit = limit;
      this.autoId = autoId;
      this.autoSeq = autoSeq;
      this.idMs = idMs;
      this.idSeq = idSeq;
      this.fields = fields;
      this.clockMs = clockMs;
   }

   @Override
   public byte[] apply(EntryView.ReadWriteEntryView<byte[], StreamBucket> entryView) {
      StreamBucket bucket = entryView.peek().orElse(null);
      if (bucket == null) {
         if (nomkstream) {
            return null;
         }
         bucket = new StreamBucket();
      }

      StreamID id = generateId(bucket);
      bucket.append(id, fields);
      trim(bucket);
      entryView.set(bucket);
      return id.asBytes();
   }

   private StreamID generateId(StreamBucket bucket) {
      StreamID lastId = bucket.lastId();
      if (autoId) {
         // The '*' ID: use the current time, or the last ID time when it is in the future. When the millisecond part
         // matches the last ID, increment the sequence part, rolling over to the next millisecond on overflow. The
         // clock is provided by the originator of the command: the function must be deterministic, as it executes on
         // all the owners of the key, which may have unsynchronized clocks.
         long ms = clockMs;
         if (Long.compareUnsigned(lastId.ms(), ms) > 0) {
            ms = lastId.ms();
         }
         long seq;
         if (ms == lastId.ms()) {
            seq = lastId.seq() + 1;
            if (seq == 0) {
               // Sequence part overflowed, move to the next millisecond.
               ms++;
               if (ms == 0) {
                  // Millisecond part overflowed: the stream exhausted the last possible ID.
                  throw new RespCommandException(
                        "ERR The stream has exhausted the last possible ID, unable to generate more entries");
               }
               seq = 0;
            }
         } else {
            seq = 0;
         }
         return new StreamID(ms, seq);
      }

      if (autoSeq) {
         // The '<ms>-*' ID: the milliseconds part is explicit, the sequence part is generated.
         if (Long.compareUnsigned(idMs, lastId.ms()) < 0) {
            throw new RespCommandException("ERR The ID specified in XADD is equal or smaller than the last one");
         }
         if (idMs == lastId.ms()) {
            if (lastId.seq() == -1L) {
               // The sequence part cannot overflow the unsigned 64-bit range.
               throw new RespCommandException("ERR The ID specified in XADD is equal or smaller than the last one");
            }
            return new StreamID(idMs, lastId.seq() + 1);
         }
         return new StreamID(idMs, 0);
      }

      // The explicit ID: must be greater than 0-0 and greater than the last ID in the stream.
      StreamID id = new StreamID(idMs, idSeq);
      if (id.compareTo(StreamID.ZERO) <= 0) {
         throw new RespCommandException("ERR The ID specified in XADD must be greater than 0-0");
      }
      if (id.compareTo(lastId) <= 0) {
         throw new RespCommandException("ERR The ID specified in XADD is equal or smaller than the last one");
      }
      return id;
   }

   private void trim(StreamBucket bucket) {
      switch (trimStrategy) {
         case TRIM_MAXLEN:
            bucket.trimToMaxlen(maxlen, limit);
            break;
         case TRIM_MINID:
            bucket.trimToMinId(minId, limit);
            break;
         default:
            // No trimming.
            break;
      }
   }
}
