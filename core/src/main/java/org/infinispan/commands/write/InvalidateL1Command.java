package org.infinispan.commands.write;

import java.util.Collection;
import java.util.Collections;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.Visitor;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.context.InvocationContext;
import org.infinispan.marshall.protostream.impl.MarshallableArray;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;

/**
 * Invalidates an entry in a L1 cache (used with DIST mode)
 *
 * @author Manik Surtani
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
@ProtoTypeId(ProtoStreamTypeIds.INVALIDATE_L1_COMMAND)
public class InvalidateL1Command extends InvalidateCommand {
   public static final int COMMAND_ID = 7;

   @ProtoField(number = 5, javaType = JGroupsAddress.class)
   final Address writeOrigin;

   @ProtoFactory
   InvalidateL1Command(long flagsWithoutRemote, int topologyId, CommandInvocationId commandInvocationId,
                       MarshallableArray<Object> wrappedKeys, JGroupsAddress writeOrigin) {
      super(flagsWithoutRemote, topologyId, commandInvocationId, wrappedKeys);
      this.writeOrigin = writeOrigin;
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
