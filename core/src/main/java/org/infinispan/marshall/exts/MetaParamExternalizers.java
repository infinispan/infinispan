package org.infinispan.marshall.exts;

import org.infinispan.commons.api.functional.EntryVersion;
import org.infinispan.commons.api.functional.EntryVersion.NumericEntryVersion;
import org.infinispan.commons.api.functional.MetaParam.EntryVersionParam;
import org.infinispan.commons.api.functional.MetaParam.Lifespan;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

public final class MetaParamExternalizers {

   private MetaParamExternalizers() {
      // Do not instantiate
   }

   public static final class LifespanExternalizer extends AbstractExternalizer<Lifespan> {
      @Override
      public void writeObject(ObjectOutput output, Lifespan object) throws IOException {
         output.writeLong(object.get());
      }

      @Override
      public Lifespan readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new Lifespan(input.readLong());
      }

      @Override
      public Set<Class<? extends Lifespan>> getTypeClasses() {
         return Util.<Class<? extends Lifespan>>asSet(Lifespan.class);
      }

      @Override
      public Integer getId() {
         return Ids.META_LIFESPAN;
      }
   }

   public static final class EntryVersionParamExternalizer extends AbstractExternalizer<EntryVersionParam> {
      @Override
      public void writeObject(ObjectOutput output, EntryVersionParam object) throws IOException {
         output.writeObject(object.get());
      }

      @Override
      public EntryVersionParam readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         EntryVersion entryVersion = (EntryVersion) input.readObject();
         return new EntryVersionParam(entryVersion);
      }

      @Override
      public Set<Class<? extends EntryVersionParam>> getTypeClasses() {
         return Util.<Class<? extends EntryVersionParam>>asSet(EntryVersionParam.class);
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
