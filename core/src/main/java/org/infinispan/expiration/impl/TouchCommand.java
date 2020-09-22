package org.infinispan.expiration.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.infinispan.commands.Visitor;
import org.infinispan.commands.read.AbstractDataCommand;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;

/**
 * This command updates a cache entry's last access timestamp. If eviction is enabled, it will also update the recency information
 * <p>
 * This command returns a Boolean that is whether this command was able to touch the value or not.
 */
public class TouchCommand extends AbstractDataCommand {
   public static final byte COMMAND_ID = 66;

   private boolean touchEvenIfExpired;

   public TouchCommand() { }

   public TouchCommand(Object key, int segment, long flagBitSet, boolean touchEvenIfExpired) {
      super(key, segment, flagBitSet);
      this.touchEvenIfExpired = touchEvenIfExpired;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeObject(key);
      UnsignedNumeric.writeUnsignedInt(output, segment);
      output.writeLong(FlagBitSets.copyWithoutRemotableFlags(getFlagsBitSet()));
      output.writeBoolean(touchEvenIfExpired);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      key = input.readObject();
      segment = UnsignedNumeric.readUnsignedInt(input);
      setFlagsBitSet(input.readLong());
      touchEvenIfExpired = input.readBoolean();
   }


   public boolean isTouchEvenIfExpired() {
      return touchEvenIfExpired;
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitTouchCommand(ctx, this);
   }

   @Override
   public LoadType loadType() {
      return LoadType.DONT_LOAD;
   }
}
