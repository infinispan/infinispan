package org.infinispan.commands.remote;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.infinispan.commands.AbstractTopologyAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;

/**
 * {@link org.infinispan.commands.VisitableCommand} that fetches the keys belonging to a group.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class GetKeysInGroupCommand extends AbstractTopologyAffectedCommand implements VisitableCommand {

   public static final byte COMMAND_ID = 43;

   private Object groupName;
   /*
   local state to avoid checking everywhere if the node in which this command is executed is the group owner.
    */
   private transient boolean isGroupOwner;

   public GetKeysInGroupCommand(long flagsBitSet, Object groupName) {
      this.groupName = groupName;
      setFlagsBitSet(flagsBitSet);
   }

   public GetKeysInGroupCommand() {
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeObject(groupName);
      output.writeLong(FlagBitSets.copyWithoutRemotableFlags(getFlagsBitSet()));
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      groupName = input.readObject();
      setFlagsBitSet(input.readLong());
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public boolean canBlock() {
      return false;
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitGetKeysInGroupCommand(ctx, this);
   }

   @Override
   public LoadType loadType() {
      return LoadType.OWNER;
   }

   public Object getGroupName() {
      return groupName;
   }

   @Override
   public String toString() {
      return "GetKeysInGroupCommand{" +
            "groupName='" + groupName + '\'' +
            ", flags=" + printFlags() +
            '}';
   }

   public boolean isGroupOwner() {
      return isGroupOwner;
   }

   public void setGroupOwner(boolean isGroupOwner) {
      this.isGroupOwner = isGroupOwner;
   }
}
