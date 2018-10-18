package org.infinispan.commands.write;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Collections;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.Visitor;
import org.infinispan.context.InvocationContext;
import org.infinispan.remoting.transport.Address;

/**
 * Invalidates an entry in a L1 cache (used with DIST mode)
 *
 * @author Manik Surtani
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class InvalidateL1Command extends InvalidateCommand {
   public static final int COMMAND_ID = 7;
   private Address writeOrigin;

   public InvalidateL1Command() {
      writeOrigin = null;
   }

   public InvalidateL1Command(long flagsBitSet,
                              CommandInvocationId commandInvocationId, Object... keys) {
      super(flagsBitSet, commandInvocationId, keys);
      writeOrigin = null;
   }

   public InvalidateL1Command(long flagsBitSet, Collection<Object> keys, CommandInvocationId commandInvocationId) {
      this(null, flagsBitSet, keys, commandInvocationId);
   }

   public InvalidateL1Command(Address writeOrigin, long flagsBitSet, Collection<Object> keys,
         CommandInvocationId commandInvocationId) {
      super(flagsBitSet, keys, commandInvocationId);
      this.writeOrigin = writeOrigin;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   public void setKeys(Object[] keys) {
      this.keys = keys;
   }

   @Override
   public Collection<?> getKeysToLock() {
      //no keys to lock
      return Collections.emptyList();
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      super.writeTo(output); //command invocation id + keys
      output.writeObject(writeOrigin);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      super.readFrom(input);
      writeOrigin = (Address) input.readObject();
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitInvalidateL1Command(ctx, this);
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() + "{" +
            "num keys=" + (keys == null ? 0 : keys.length) +
            ", origin=" + writeOrigin +
            '}';
   }

   /**
    * Returns true if the write that caused the invalidation was performed on this node.
    * More formal, if a put(k) happens on node A and ch(A)={B}, then an invalidation message
    * might be multicasted by B to all cluster members including A. This method returns true
    * if and only if the node where it is invoked is A.
    */
   public boolean isCausedByALocalWrite(Address address) {
      return writeOrigin != null && writeOrigin.equals(address);
   }
}
