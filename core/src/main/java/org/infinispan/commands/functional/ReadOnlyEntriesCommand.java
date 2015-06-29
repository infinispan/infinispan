package org.infinispan.commands.functional;

import org.infinispan.commands.Visitor;
import org.infinispan.commands.read.AbstractDataCommand;
import org.infinispan.context.InvocationContext;

public class ReadOnlyEntriesCommand extends AbstractDataCommand {

   public static final byte COMMAND_ID = 46;

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      // TODO: Customise this generated block
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public byte getCommandId() {
      return 0;  // TODO: Customise this generated block
   }

   @Override
   public Object[] getParameters() {
      return new Object[0];  // TODO: Customise this generated block
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return null;  // TODO: Customise this generated block
   }
}
