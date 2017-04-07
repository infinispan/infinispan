package org.infinispan.commands;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Optional;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.ByteBufferImpl;
import org.infinispan.commons.io.ExposedByteArrayInputStream;
import org.infinispan.commons.io.ExposedByteArrayOutputStream;
import org.infinispan.commons.marshall.StreamingMarshaller;

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
   private long timestamp;
   // Allows chaining instead of storing in separate collection
   private InvocationRecord next;

   public InvocationRecord(CommandInvocationId id, Object returnValue, boolean authoritative, boolean created, boolean modified, boolean removed, long timestamp, InvocationRecord next) {
      this(id, returnValue, (byte) ((authoritative ? AUTHORITATIVE : 0) + (created ? CREATED : 0) + (modified ? MODIFIED : 0) + (removed ? REMOVED : 0)), timestamp, next);
   }

   private InvocationRecord(CommandInvocationId id, Object returnValue, byte flags, long timestamp, InvocationRecord next) {
      assert id != CommandInvocationId.DUMMY_INVOCATION_ID;
      assertDoesNotRepeat(id);
      this.id = id;
      this.flags = flags;
      this.timestamp = timestamp;
      this.returnValue = returnValue;
      this.next = next;
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

   public static InvocationRecord lookup(InvocationRecord record, CommandInvocationId id) {
      while (record != null) {
         if (id.equals(record.id)) {
            return record;
         }
         record = record.next;
      }
      return null;
   }

   public static Optional<InvocationRecord> find(InvocationRecord record, CommandInvocationId id) {
      while (record != null) {
         if (id.equals(record.id)) {
            return Optional.of(record);
         }
         record = record.next;
      }
      return Optional.empty();
   }

   @Override
   public String toString() {
      StringBuilder sb = new StringBuilder();
      InvocationRecord record = this;
      do {
         if (record != this) {
            sb.append(", ");
         }
         sb.append('[').append(id.getAddress()).append(':').append(id.getId())
               .append(':')
               .append((flags & AUTHORITATIVE) == AUTHORITATIVE ? 'A' : 'N')
               .append(cmrType())
               .append(':').append(timestamp)
               .append(" -> ").append(returnValue).append(']');
         record = record.next;
      } while (record != null);
      return sb.toString();
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
