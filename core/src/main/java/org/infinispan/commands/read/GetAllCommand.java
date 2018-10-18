package org.infinispan.commands.read;

import static org.infinispan.commons.util.Util.toStr;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;

import org.infinispan.commands.AbstractTopologyAffectedCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;

/**
 * Retrieves multiple entries at once.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class GetAllCommand extends AbstractTopologyAffectedCommand {
   public static final byte COMMAND_ID = 44;

   private Collection<?> keys;
   private boolean returnEntries;

   public GetAllCommand(Collection<?> keys, long flagsBitSet, boolean returnEntries) {
      this.keys = keys;
      this.returnEntries = returnEntries;
      setFlagsBitSet(flagsBitSet);
   }

   GetAllCommand() {
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitGetAllCommand(ctx, this);
   }

   @Override
   public LoadType loadType() {
      return LoadType.PRIMARY;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      MarshallUtil.marshallCollection(keys, output);
      output.writeLong(FlagBitSets.copyWithoutRemotableFlags(getFlagsBitSet()));
      output.writeBoolean(returnEntries);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      keys = MarshallUtil.unmarshallCollection(input, ArrayList::new);
      setFlagsBitSet(input.readLong());
      returnEntries = input.readBoolean();
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public boolean canBlock() {
      return false;
   }

   public boolean isReturnEntries() {
      return returnEntries;
   }

   public Collection<?> getKeys() {
      return keys;
   }

   public void setKeys(Collection<?> keys) {
      this.keys = keys;
   }

   @Override
   public String toString() {
      final StringBuilder sb = new StringBuilder("GetAllCommand{");
      sb.append("keys=").append(toStr(keys));
      sb.append(", returnEntries=").append(returnEntries);
      sb.append(", flags=").append(printFlags());
      sb.append('}');
      return sb.toString();
   }
}
