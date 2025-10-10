package org.infinispan.remoting.transport.jgroups;

import java.util.function.Supplier;

import org.jgroups.BytesMessage;
import org.jgroups.Message;

// TODO: we don't actually use this yet, just reserve the type id for future usage
class InfinispanMessage extends BytesMessage {
   public static final short TYPE = 1234;

   public InfinispanMessage() {
   }

   public InfinispanMessage(org.jgroups.Address dest) {
      super(dest);
   }

   public InfinispanMessage(org.jgroups.Address dest, byte[] array) {
      super(dest, array);
   }

   @Override
   public Supplier<Message> create() {
      return InfinispanMessage::new;
   }

   @Override
   public short getType() {
      return TYPE;
   }
}
