package org.infinispan.interceptors.distribution;

import java.io.IOException;
import java.util.Set;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.commons.marshall.UserObjectInput;
import org.infinispan.commons.marshall.UserObjectOutput;
import org.infinispan.commons.util.Util;
import org.infinispan.container.versioning.EntryVersion;

public class VersionedResult {
   public final Object result;
   public final EntryVersion version;

   public VersionedResult(Object result, EntryVersion version) {
      this.result = result;
      this.version = version;
   }

   @Override
   public String toString() {
      return new StringBuilder("VersionedResult{").append(result).append(" (").append(version).append(")}").toString();
   }

   public static class Externalizer implements AdvancedExternalizer<VersionedResult> {
      @Override
      public Set<Class<? extends VersionedResult>> getTypeClasses() {
         return Util.asSet(VersionedResult.class);
      }

      @Override
      public Integer getId() {
         return Ids.VERSIONED_RESULT;
      }

      @Override
      public void writeObject(UserObjectOutput output, VersionedResult object) throws IOException {
         output.writeObject(object.result);
         output.writeObject(object.version);
      }

      @Override
      public VersionedResult readObject(UserObjectInput input) throws IOException, ClassNotFoundException {
         return new VersionedResult(input.readObject(), (EntryVersion) input.readObject());
      }
   }
}
