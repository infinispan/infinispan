package org.infinispan.marshall.exts;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.distribution.util.ReadOnlySegmentAwareCollection;
import org.infinispan.marshall.core.Ids;
import org.jboss.marshalling.util.IdentityIntMap;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

public class CollectionExternalizer implements AdvancedExternalizer<Collection> {
   private static final int READ_ONLY_SEGMENT_AWARE_COLLECTION = 0;
   private final IdentityIntMap<Class<?>> numbers = new IdentityIntMap<>(1);

   public CollectionExternalizer() {
      numbers.put(ReadOnlySegmentAwareCollection.class, READ_ONLY_SEGMENT_AWARE_COLLECTION);
   }

   @Override
   public void writeObject(ObjectOutput output, Collection collection) throws IOException {
      int number = numbers.get(collection.getClass(), -1);
      output.writeByte(number);
      MarshallUtil.marshallCollection(collection, output);
   }

   @Override
   public Collection readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      int magicNumber = input.readUnsignedByte();
      switch (magicNumber) {
         case READ_ONLY_SEGMENT_AWARE_COLLECTION:
            return MarshallUtil.unmarshallCollection(input, ArrayList::new);
         default:
            throw new IllegalStateException("Unknown Set type: " + magicNumber);
      }
   }

   @Override
   public Integer getId() {
      return Ids.COLLECTIONS;
   }

   @Override
   public Set<Class<? extends Collection>> getTypeClasses() {
      return Util.asSet(ReadOnlySegmentAwareCollection.class);
   }

}
