package org.infinispan.commons.marshall.exts;

import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.commons.equivalence.AnyServerEquivalence;
import org.infinispan.commons.equivalence.ByteArrayEquivalence;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.commons.util.Util;
import org.jboss.marshalling.util.IdentityIntMap;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

public final class EquivalenceExternalizer extends AbstractExternalizer<Equivalence> {

   private static final int BYTE_ARRAY_EQ    = 0x00;
   private static final int ANY_EQ           = 0x01;
   private static final int ANY_SERVER_EQ    = 0x02;

   private final IdentityIntMap<Class<?>> subIds = new IdentityIntMap<>(2);

   public EquivalenceExternalizer() {
      subIds.put(ByteArrayEquivalence.class, BYTE_ARRAY_EQ);
      subIds.put(AnyEquivalence.class, ANY_EQ);
      subIds.put(AnyServerEquivalence.class, ANY_SERVER_EQ);
   }

   @Override
   public Set<Class<? extends Equivalence>> getTypeClasses() {
      return Util.asSet(ByteArrayEquivalence.class, AnyEquivalence.class,
            AnyServerEquivalence.class);
   }

   @Override
   public Integer getId() {
      return Ids.EQUIVALENCE;
   }

   @Override
   public void writeObject(ObjectOutput out, Equivalence obj) throws IOException {
      int subId = subIds.get(obj.getClass(), -1);
      out.writeByte(subId);
   }

   @Override
   public Equivalence readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      int subId = input.readUnsignedByte();
      switch (subId) {
         case BYTE_ARRAY_EQ:
            return ByteArrayEquivalence.INSTANCE;
         case ANY_EQ:
            return AnyEquivalence.getInstance();
         case ANY_SERVER_EQ:
            return AnyServerEquivalence.INSTANCE;
         default:
            throw new IllegalStateException("Unknown equivalence type: " + Integer.toHexString(subId));
      }
   }

}
