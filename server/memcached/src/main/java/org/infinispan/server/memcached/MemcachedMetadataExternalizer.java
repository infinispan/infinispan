package org.infinispan.server.memcached;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.infinispan.commands.InvocationRecord;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.container.versioning.EntryVersion;
import org.jboss.marshalling.util.IdentityIntMap;

/**
 * @author wburns
 * @since 9.0
 */
public class MemcachedMetadataExternalizer extends AbstractExternalizer<MemcachedMetadata> {

   final static int Immortal = 0;
   final static int Expirable = 1;

   final static IdentityIntMap<Class> numbers = new IdentityIntMap<>(2);

   static {
      numbers.put(MemcachedMetadata.class, Immortal);
      numbers.put(MemcachedExpirableMetadata.class, Expirable);
   }

   @Override
   public Set<Class<? extends MemcachedMetadata>> getTypeClasses() {
      return Util.asSet(MemcachedMetadata.class, MemcachedExpirableMetadata.class);
   }

   @Override
   public void writeObject(ObjectOutput output, MemcachedMetadata object) throws IOException {
      output.writeLong(object.flags);
      output.writeObject(object.version);
      InvocationRecord.writeListTo(output, object.records);
      int number = numbers.get(object.getClass(), -1);
      output.write(number);
      if (number == Expirable) {
         output.writeLong(object.lifespan());
      }
   }

   @Override
   public MemcachedMetadata readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      long flags = input.readLong();
      EntryVersion version = (EntryVersion) input.readObject();
      InvocationRecord records = InvocationRecord.readListFrom(input);
      int number = input.readUnsignedByte();
      switch (number) {
         case Immortal:
            return new MemcachedMetadata(flags, version, records);
         case Expirable:
            long lifespan = input.readLong();
            return new MemcachedExpirableMetadata(flags, version, records, lifespan, TimeUnit.MILLISECONDS);
         default:
            throw new IllegalArgumentException("Number " + number + " not supported!");
      }
   }
}
