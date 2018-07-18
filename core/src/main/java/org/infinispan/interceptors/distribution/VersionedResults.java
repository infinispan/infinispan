package org.infinispan.interceptors.distribution;

import java.io.IOException;
import java.util.Set;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.commons.marshall.UserObjectInput;
import org.infinispan.commons.marshall.UserObjectOutput;
import org.infinispan.commons.util.Util;
import org.infinispan.container.versioning.EntryVersion;

public class VersionedResults {
   public final Object[] values;
   public final EntryVersion[] versions;

   public VersionedResults(Object[] values, EntryVersion[] versions) {
      this.values = values;
      this.versions = versions;
   }

   @Override
   public String toString() {
      StringBuilder sb = new StringBuilder("VersionedResults{");
      for (int i = 0; i < values.length; ++i) {
         sb.append(values[i]).append(" (").append(versions[i]).append(')');
         if (i != values.length - 1) sb.append(", ");
      }
      sb.append('}');
      return sb.toString();
   }

   public static class Externalizer implements AdvancedExternalizer<VersionedResults> {

      @Override
      public Set<Class<? extends VersionedResults>> getTypeClasses() {
         return Util.asSet(VersionedResults.class);
      }

      @Override
      public Integer getId() {
         return Ids.VERSIONED_RESULTS;
      }

      @Override
      public void writeObject(UserObjectOutput output, VersionedResults object) throws IOException {
         output.writeInt(object.values.length);
         // TODO: we could optimize this if all objects are of the same type
         for (Object value : object.values) output.writeObject(value);
         for (EntryVersion version : object.versions) output.writeObject(version);
      }

      @Override
      public VersionedResults readObject(UserObjectInput input) throws IOException, ClassNotFoundException {
         int length = input.readInt();
         Object[] values = new Object[length];
         for (int i = 0; i < length; ++i) {
            values[i] = input.readObject();
         }
         EntryVersion[] versions = new EntryVersion[length];
         for (int i = 0; i < length; ++i) {
            versions[i] = (EntryVersion) input.readObject();
         }
         return new VersionedResults(values, versions);
      }
   }
}
