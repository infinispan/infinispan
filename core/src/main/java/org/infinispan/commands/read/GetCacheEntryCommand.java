package org.infinispan.commands.read;

import static org.infinispan.commons.util.Util.toStr;

import java.io.IOException;
import java.io.ObjectInput;

import org.infinispan.commands.Visitor;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.marshall.MarshalledEntryUtil;
import org.infinispan.marshall.core.MarshalledEntryFactory;
import org.infinispan.commons.marshall.UserObjectOutput;

/**
 * Used to fetch a full CacheEntry rather than just the value.
 * This functionality was originally incorporated into GetKeyValueCommand.
 *
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2014 Red Hat Inc.
 * @since 7.1
 */
public final class GetCacheEntryCommand extends AbstractDataCommand {

   public static final byte COMMAND_ID = 45;

   private InternalEntryFactory entryFactory;

   public GetCacheEntryCommand(Object key, int segment, long flagsBitSet, InternalEntryFactory entryFactory) {
      super(key, segment, flagsBitSet);
      this.entryFactory = entryFactory;
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
   public Object perform(InvocationContext ctx) throws Throwable {
      CacheEntry entry = ctx.lookupEntry(key);
      if (entry.isNull() || entry.isRemoved()) {
         return null;
      }

      return entryFactory.copy(entry);
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(UserObjectOutput output, MarshalledEntryFactory entryFactory) throws IOException {
      output.writeKey(key);
      UnsignedNumeric.writeUnsignedInt(output, segment);
      output.writeLong(FlagBitSets.copyWithoutRemotableFlags(getFlagsBitSet()));
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      key = MarshalledEntryUtil.readKey(input);
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
