package org.infinispan.marshall.exts;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import org.infinispan.commons.api.functional.EntryVersion;
import org.infinispan.commons.api.functional.EntryVersion.NumericEntryVersion;
import org.infinispan.commons.api.functional.MetaParam.MetaEntryVersion;
import org.infinispan.commons.api.functional.MetaParam.MetaLifespan;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;

public final class MetaParamExternalizers {

   private MetaParamExternalizers() {
      // Do not instantiate
   }

   public static final class LifespanExternalizer extends AbstractExternalizer<MetaLifespan> {
      @Override
      public void writeObject(ObjectOutput output, MetaLifespan object) throws IOException {
         output.writeLong(object.get());
      }

      @Override
      public MetaLifespan readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new MetaLifespan(input.readLong());
      }

      @Override
      public Set<Class<? extends MetaLifespan>> getTypeClasses() {
         return Util.<Class<? extends MetaLifespan>>asSet(MetaLifespan.class);
      }

      @Override
      public Integer getId() {
         return Ids.META_LIFESPAN;
      }
   }

   public static final class EntryVersionParamExternalizer extends AbstractExternalizer<MetaEntryVersion> {
      @Override
      public void writeObject(ObjectOutput output, MetaEntryVersion object) throws IOException {
         output.writeObject(object.get());
      }

      @Override
      public MetaEntryVersion readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         EntryVersion entryVersion = (EntryVersion) input.readObject();
         return new MetaEntryVersion(entryVersion);
      }

      @Override
      public Set<Class<? extends MetaEntryVersion>> getTypeClasses() {
         return Util.<Class<? extends MetaEntryVersion>>asSet(MetaEntryVersion.class);
      }

      @Override
      public Integer getId() {
         return Ids.META_ENTRY_VERSION;
      }
   }

   public static final class NumericEntryVersionExternalizer extends AbstractExternalizer<NumericEntryVersion> {
      @Override
      public void writeObject(ObjectOutput output, NumericEntryVersion object) throws IOException {
         output.writeLong(object.get());
      }

      @Override
      public NumericEntryVersion readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         long version = input.readLong();
         return new NumericEntryVersion(version);
      }

      @Override
      public Set<Class<? extends NumericEntryVersion>> getTypeClasses() {
         return Util.<Class<? extends NumericEntryVersion>>asSet(NumericEntryVersion.class);
      }

      @Override
      public Integer getId() {
         return Ids.NUMERIC_ENTRY_VERSION;
      }
   }

}
