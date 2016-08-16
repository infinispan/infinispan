package org.infinispan.marshall.exts;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;
import org.jboss.marshalling.util.IdentityIntMap;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.Set;

public final class QueueExternalizers extends AbstractExternalizer<Queue> {

   private static final int ARRAY_DEQUE = 0x00;

   private final IdentityIntMap<Class<? extends Queue>> subIds = new IdentityIntMap<>(2);

   public QueueExternalizers() {
      subIds.put(ArrayDeque.class, ARRAY_DEQUE);
   }

   @Override
   public Set<Class<? extends Queue>> getTypeClasses() {
      return Util.asSet(ArrayDeque.class);
   }

   @Override
   public Integer getId() {
      return Ids.QUEUE;
   }

   @Override
   public void writeObject(ObjectOutput out, Queue obj) throws IOException {
      int subId = subIds.get(obj.getClass(), -1);
      out.writeByte(subId);
      MarshallUtil.marshallCollection(obj, out);
   }

   @Override
   public Queue readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      int subId = input.readUnsignedByte();
      switch (subId) {
         case ARRAY_DEQUE:
            return MarshallUtil.unmarshallCollection(input, ArrayDeque::new);
         default:
            throw new IllegalStateException("Unknown Queue type: " + Integer.toHexString(subId));
      }
   }

}
