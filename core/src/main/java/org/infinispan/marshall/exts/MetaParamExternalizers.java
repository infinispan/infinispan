package org.infinispan.marshall.exts;

import java.io.IOException;
import java.io.ObjectInput;
import java.util.Set;

import org.infinispan.commons.marshall.UserObjectOutput;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.functional.MetaParam.MetaEntryVersion;
import org.infinispan.functional.MetaParam.MetaLifespan;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.functional.MetaParam.MetaMaxIdle;
import org.infinispan.marshall.core.Ids;

public final class MetaParamExternalizers {

   private MetaParamExternalizers() {
      // Do not instantiate
   }

   public static final class LifespanExternalizer extends AbstractExternalizer<MetaLifespan> {
      @Override
      public void writeObject(UserObjectOutput output, MetaLifespan object) throws IOException {
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

   public static final class MaxIdleExternalizer extends AbstractExternalizer<MetaMaxIdle> {
      @Override
      public void writeObject(UserObjectOutput output, MetaMaxIdle object) throws IOException {
         output.writeLong(object.get());
      }

      @Override
      public MetaMaxIdle readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new MetaMaxIdle(input.readLong());
      }

      @Override
      public Set<Class<? extends MetaMaxIdle>> getTypeClasses() {
         return Util.<Class<? extends MetaMaxIdle>>asSet(MetaMaxIdle.class);
      }

      @Override
      public Integer getId() {
         return Ids.META_MAX_IDLE;
      }
   }

   public static final class EntryVersionParamExternalizer extends AbstractExternalizer<MetaEntryVersion> {
      @Override
      public void writeObject(UserObjectOutput output, MetaEntryVersion object) throws IOException {
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

}
