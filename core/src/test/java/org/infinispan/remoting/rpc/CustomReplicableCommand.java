package org.infinispan.remoting.rpc;

import java.io.IOException;
import java.io.Serializable;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.commons.marshall.UserObjectInput;
import org.infinispan.context.InvocationContext;
import org.infinispan.marshall.core.MarshalledEntryFactory;
import org.infinispan.commons.marshall.UserObjectOutput;

/**
 * @author anistor@redhat.com
 * @since 5.3
 */
public class CustomReplicableCommand implements VisitableCommand, Serializable {

   public static final byte COMMAND_ID = 127;

   private static final long serialVersionUID = -1L;

   private Object arg;

   public CustomReplicableCommand() {
      // For command id uniqueness test
   }

   public CustomReplicableCommand(Object arg) {
      this.arg = arg;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      if (arg instanceof Throwable) {
         throw (Throwable) arg;
      }

      // echo the arg back to the caller
      return arg;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(UserObjectOutput output, MarshalledEntryFactory entryFactory) throws IOException {
      output.writeObject(arg);
   }

   @Override
   public void readFrom(UserObjectInput input) throws IOException, ClassNotFoundException {
      arg = input.readObject();
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
      return visitor.visitUnknownCommand(ctx, this);
   }

   @Override
   public LoadType loadType() {
      throw new UnsupportedOperationException();
   }

}
