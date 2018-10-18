package org.infinispan.commands.read;

import static org.infinispan.commons.util.Util.toStr;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.infinispan.commands.Visitor;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;

/**
 * Used to fetch a full CacheEntry rather than just the value.
 * This functionality was originally incorporated into GetKeyValueCommand.
 *
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2014 Red Hat Inc.
 * @since 7.1
 */
public final class GetCacheEntryCommand extends AbstractDataCommand {

   public static final byte COMMAND_ID = 45;

   public GetCacheEntryCommand(Object key, int segment, long flagsBitSet) {
      super(key, segment, flagsBitSet);
   }

   public GetCacheEntryCommand() {
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitGetCacheEntryCommand(ctx, this);
   }

   @Override
   public LoadType loadType() {
      return LoadType.OWNER;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeObject(key);
      UnsignedNumeric.writeUnsignedInt(output, segment);
      output.writeLong(FlagBitSets.copyWithoutRemotableFlags(getFlagsBitSet()));
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      key = input.readObject();
      segment = UnsignedNumeric.readUnsignedInt(input);
      setFlagsBitSet(input.readLong());
   }

   public String toString() {
      return new StringBuilder()
            .append("GetCacheEntryCommand {key=")
            .append(toStr(key))
            .append(", flags=").append(printFlags())
            .append("}")
            .toString();
   }

}
