package org.infinispan.commands;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.ByteBufferImpl;
import org.infinispan.commons.io.ExposedByteArrayInputStream;
import org.infinispan.commons.io.ExposedByteArrayOutputStream;
import org.infinispan.commons.marshall.StreamingMarshaller;

/**
 * This class can produce chains, not requiring any other object instances to represent the collection nor nodes.
 *
 * While entries in datacontainer are updated under DC's lock, the instances of this class are shared across different
 * {@link org.infinispan.metadata.Metadata} instances and therefore these need to be safe under concurrent access
 * for these operations:
 *
 * a) attaching a new record on the beginning of the chain: if the next node is removed concurrently, the operation
 *    results in the invalidated entry staying in the chain. As such record would be invalidated by setting timestamp
 *    to {@link Long#MIN_VALUE}, the record should be removed after next purge.
 * b) removing record from the list: concurrent removal could set {@link #next} to a node further down the chain, but
 *    all the nodes in between must be already invalid
 * c) iterating through the list
 *
 * There are three sources of invocation invalidations:
 * 1) primary/backup writer checks expiration
 * 2) expiration thread checks expiration
 * 3) originator-triggered invalidation
 *
 * There can't be any concurrent 1) expirations, and 2) should occur in single thread, too
 */
public final class InvocationRecord {
   // Set if this command was written when the node was primary owner of given entry
   private static final int AUTHORITATIVE = 1;
   private static final int CREATED = 2;
   private static final int MODIFIED = 4;
   private static final int REMOVED = 6;

   public final CommandInvocationId id;
   // TODO: maybe it would be better to store previous value and metadata and let the command run again
   public final Object returnValue;
   private byte flags;
   private volatile long timestamp;
   // Allows chaining instead of storing in separate collection
   private volatile InvocationRecord next;

   public InvocationRecord(CommandInvocationId id, Object returnValue, boolean authoritative, boolean created, boolean modified, boolean removed, long timestamp, InvocationRecord next) {
      this(id, returnValue, (byte) ((authoritative ? AUTHORITATIVE : 0) + (created ? CREATED : 0) + (modified ? MODIFIED : 0) + (removed ? REMOVED : 0)), timestamp, next);
   }

   private InvocationRecord(CommandInvocationId id, Object returnValue, byte flags, long timestamp, InvocationRecord next) {
      assert id != CommandInvocationId.DUMMY_INVOCATION_ID;
      assert timestamp != 0; // probably an error
      assertDoesNotRepeat(id);
      this.id = id;
      this.flags = flags;
      this.timestamp = timestamp;
      this.returnValue = returnValue;
      this.next = next;
   }

   public static boolean hasExpired(InvocationRecord record, long limitTime) {
      return findFirst(record, r -> r.timestamp < limitTime, r -> Boolean.TRUE, () -> Boolean.FALSE);
   }

   public static InvocationRecord purgeExpired(InvocationRecord record, long limitTime) {
      return purge(record, r -> r.timestamp < limitTime);

   }

   public static InvocationRecord purgeCompleted(InvocationRecord record, Predicate<CommandInvocationId> purgeFilter) {
      return purge(record, r -> purgeFilter.test(r.getId()));
   }

   private static InvocationRecord purge(InvocationRecord record, Predicate<InvocationRecord> purgeFilter) {
      // this could be implemented using recursion but Java does not have tail-recursion loop optimization
      InvocationRecord first = record, prev;
      while (first != null && purgeFilter.test(first)) {
         first.timestamp = Long.MIN_VALUE;
         first = first.next;
      }
      prev = first;
      if (first == null) {
         return null;
      }
      record = first.next;
      while (record != null) {
         if (purgeFilter.test(record)) {
            record.timestamp = Long.MIN_VALUE;
            prev.next = record.next;
         } else {
            prev = record;
         }
         record = record.next;
      }
      return first;
   }


   private boolean assertDoesNotRepeat(CommandInvocationId id) {
      assert next == null || (!id.equals(next.id) && next.assertDoesNotRepeat(id));
      return true;
   }

   public CommandInvocationId getId() {
      return id;
   }

   public boolean isAuthoritative() {
      return (flags & AUTHORITATIVE) == AUTHORITATIVE;
   }

   public void setAuthoritative() {
      flags |= AUTHORITATIVE;
   }

   public boolean isCreated() {
      return (flags & CREATED) == CREATED;
   }

   public boolean isModified() {
      return (flags & MODIFIED) == MODIFIED;
   }

   public boolean isRemoved() {
      return (flags & REMOVED) == REMOVED;
   }

   public long getTimestamp() {
      return timestamp;
   }

   public void touch(long timestamp) {
      this.timestamp = timestamp;
   }

   // Only for testing purposes
   public int numRecords() {
      InvocationRecord record = this;
      int numRecords = 0;
      for (; record != null; ++numRecords) record = record.next;
      return numRecords;
   }

   private static <T> T findFirst(InvocationRecord record, Predicate<InvocationRecord> predicate, Function<InvocationRecord, T> whenFound, Supplier<T> whenNotFound) {
      // Actual invocations should be inlined
      while (record != null) {
         if (predicate.test(record)) {
            return whenFound.apply(record);
         }
         record = record.next;
      }
      return whenNotFound.get();
   }

   public static InvocationRecord lookup(InvocationRecord record, CommandInvocationId id) {
      return findFirst(record, r -> id.equals(r.id), Function.identity(), () -> null);
   }

   public static Optional<InvocationRecord> find(InvocationRecord record, CommandInvocationId id) {
      return findFirst(record, r -> id.equals(r.id), Optional::of, Optional::empty);
   }

   @Override
   public String toString() {
      StringBuilder sb = new StringBuilder();
      InvocationRecord record = this;
      int numRecords = 0;
      do {
         if (record != this) {
            sb.append(", ");
         }
         sb.append('[').append(record.id.getAddress()).append(':').append(record.id.getId())
               .append(':')
               .append((record.flags & AUTHORITATIVE) == AUTHORITATIVE ? 'A' : 'N')
               .append(cmrType())
               .append(':').append(record.timestamp)
               .append(" -> ").append(record.returnValue).append(']');
         record = record.next;
         ++numRecords;
      } while (record != null);
      return sb.insert(0, numRecords).toString();
   }

   private char cmrType() {
      switch (flags & (CREATED | MODIFIED | REMOVED)) {
         case CREATED:
            return 'C';
         case MODIFIED:
            return 'M';
         case REMOVED:
            return 'R';
         default:
            return '?';
      }
   }

   public static InvocationRecord readListFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      CommandInvocationId id = CommandInvocationId.readFrom(input);
      InvocationRecord first = null, prev = null;
      while (id != CommandInvocationId.DUMMY_INVOCATION_ID) {
         Object returnValue = input.readObject();
         byte flags = input.readByte();
         long timestamp = input.readLong();
         InvocationRecord record = new InvocationRecord(id, returnValue, flags, timestamp, null);
         if (first == null) first = record;
         if (prev != null) prev.next = record;
         prev = record;
         id = CommandInvocationId.readFrom(input);
      }
      return first;
   }

   public static InvocationRecord readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      CommandInvocationId id = CommandInvocationId.readFrom(input);
      if (id == CommandInvocationId.DUMMY_INVOCATION_ID) {
         return null;
      }
      Object returnValue = input.readObject();
      byte flags = input.readByte();
      long timestamp = input.readLong();
      return new InvocationRecord(id, returnValue, flags, timestamp, null);
   }

   public static void writeListTo(ObjectOutput output, InvocationRecord record) throws IOException {
      while (record != null) {
         CommandInvocationId.writeTo(output, record.id);
         output.writeObject(record.returnValue);
         output.writeByte(record.flags);
         output.writeLong(record.timestamp);
         record = record.next;
      }
      CommandInvocationId.writeTo(output, CommandInvocationId.DUMMY_INVOCATION_ID);
   }

   public static void writeTo(ObjectOutput output, InvocationRecord record) throws IOException {
      if (record == null) {
         CommandInvocationId.writeTo(output, CommandInvocationId.DUMMY_INVOCATION_ID);
      } else {
         CommandInvocationId.writeTo(output, record.id);
         output.writeObject(record.returnValue);
         output.writeByte(record.flags);
         output.writeLong(record.timestamp);
      }
   }

   public ByteBuffer toByteBuffer(StreamingMarshaller marshaller) throws IOException {
      // TODO: copy-less version, ideally void writeTo(byte[] buf, int offset, ...)
      int estimateSize = 32;
      ExposedByteArrayOutputStream bas = new ExposedByteArrayOutputStream(estimateSize);
      ObjectOutput oo = marshaller.startObjectOutput(bas, false, estimateSize);
      writeListTo(oo, this);
      marshaller.finishObjectOutput(oo);
      return new ByteBufferImpl(bas.getRawBuffer(), 0, bas.size());
   }

   public static InvocationRecord fromBytes(StreamingMarshaller marshaller, byte[] bytes, int offset, int length) throws IOException, ClassNotFoundException {
      ExposedByteArrayInputStream bas = new ExposedByteArrayInputStream(bytes, offset, length);
      ObjectInput oi = marshaller.startObjectInput(bas, false);
      InvocationRecord record = readListFrom(oi);
      marshaller.finishObjectInput(oi);
      return record;
   }
}
