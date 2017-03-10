package org.infinispan.functional.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Optional;
import java.util.Set;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.InvocationRecord;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.commons.util.Experimental;
import org.infinispan.commons.util.Util;
import org.infinispan.functional.MetaParam;
import org.infinispan.metadata.Metadata;

@Experimental
public class InvocationRecords implements MetaParam<InvocationRecords> {
   private final InvocationRecord records;

   protected InvocationRecords(InvocationRecord records) {
      this.records = records;
   }

   @Override
   public InvocationRecords get() {
      return this;
   }

   public Optional<InvocationRecord> invocation(CommandInvocationId id) {
      return InvocationRecord.find(records, id);
   }

   public Optional<InvocationRecord> lastInvocation() {
      return Optional.ofNullable(records);
   }

   public static InvocationRecords of(InvocationRecord records) {
      return records == null ? null : new InvocationRecords(records);
   }

   @Override
   public String toString() {
      return "InvocationRecords(" + (records == null ? "<none>" : records.toString()) + ")";
   }

   public static InvocationRecords join(CommandInvocationId id, Object previousValue, Metadata previousMetadata, long timestamp, InvocationRecords records) {
      return new InvocationRecords(new InvocationRecord(id, previousValue, previousMetadata, timestamp, records == null ? null : records.records));
   }

   public static class Externalizer implements AdvancedExternalizer<InvocationRecords> {
      @Override
      public Set<Class<? extends InvocationRecords>> getTypeClasses() {
         return Util.asSet(InvocationRecords.class);
      }

      @Override
      public Integer getId() {
         return Ids.META_PARAM_INVOCATION_RECORDS;
      }

      @Override
      public void writeObject(ObjectOutput output, InvocationRecords object) throws IOException {
         if (object == null) {
            output.writeObject(null);
            return;
         }
         InvocationRecord.writeListTo(output, object.records);
      }

      @Override
      public InvocationRecords readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         InvocationRecord records = InvocationRecord.readListFrom(input);
         return InvocationRecords.of(records);
      }
   }
}
