package org.infinispan.container.versioning;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;

public class FunctionalEntryVersionAdapter implements org.infinispan.commons.api.functional.EntryVersion<EntryVersion> {
   final EntryVersion delegate;

   public FunctionalEntryVersionAdapter(EntryVersion delegate) {
      this.delegate = delegate;
   }

   @Override
   public EntryVersion get() {
      return delegate;
   }

   @Override
   public CompareResult compareTo(org.infinispan.commons.api.functional.EntryVersion<EntryVersion> other) {
      switch (delegate.compareTo(other.get())) {
         case BEFORE:
            return CompareResult.BEFORE;
         case AFTER:
            return CompareResult.AFTER;
         case EQUAL:
            return CompareResult.EQUAL;
         case CONFLICTING:
            return CompareResult.CONFLICTING;
         default:
            throw new IllegalStateException();
      }
   }

   public static class Externalizer implements AdvancedExternalizer<FunctionalEntryVersionAdapter> {
      @Override
      public Set<Class<? extends FunctionalEntryVersionAdapter>> getTypeClasses() {
         return Util.asSet(FunctionalEntryVersionAdapter.class);
      }

      @Override
      public Integer getId() {
         return Ids.FUNCTIONAL_ENTRY_VERSION_ADAPTER;
      }

      @Override
      public void writeObject(ObjectOutput output, FunctionalEntryVersionAdapter object) throws IOException {
         output.writeObject(object.delegate);
      }

      @Override
      public FunctionalEntryVersionAdapter readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new FunctionalEntryVersionAdapter((EntryVersion) input.readObject());
      }
   }
}
