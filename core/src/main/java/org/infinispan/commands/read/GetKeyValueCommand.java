package org.infinispan.commands.read;

import static org.infinispan.commons.util.Util.toStr;

import org.infinispan.commands.Visitor;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.context.InvocationContext;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Implements functionality defined by {@link org.infinispan.Cache#get(Object)} and
 * {@link org.infinispan.Cache#containsKey(Object)} operations
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
@ProtoTypeId(ProtoStreamTypeIds.GET_KEY_VALUE_COMMAND)
public class GetKeyValueCommand extends AbstractDataCommand {

   public static final byte COMMAND_ID = 4;

   @ProtoFactory
   GetKeyValueCommand(MarshallableObject<?> wrappedKey, long flagsWithoutRemote, int topologyId, int segment) {
      super(wrappedKey, flagsWithoutRemote, topologyId, segment);
   }

   public GetKeyValueCommand(Object key, int segment, long flagsBitSet) {
      super(key, segment, flagsBitSet);
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitGetKeyValueCommand(ctx, this);
   }

   @Override
   public LoadType loadType() {
      return LoadType.OWNER;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   public String toString() {
      return "GetKeyValueCommand {key=" +
            toStr(key) +
            ", flags=" + printFlags() +
            "}";
   }
}
