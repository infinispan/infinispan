package org.infinispan.remoting.rpc;

import java.io.Serializable;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.context.InvocationContext;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

/**
 * @author anistor@redhat.com
 * @since 5.3
 */
public class CustomReplicableCommand implements VisitableCommand, Serializable {

   public static final byte COMMAND_ID = 127;

   private static final long serialVersionUID = -1L;

   final Object arg;

   public CustomReplicableCommand(Object arg) {
      this.arg = arg;
   }

   @ProtoFactory
   CustomReplicableCommand(MarshallableObject<?> arg) {
      this.arg = MarshallableObject.unwrap(arg);
   }

   @Override
   public Object invoke() throws Throwable {
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

   @ProtoField(1)
   MarshallableObject<?> getArg() {
      return MarshallableObject.create(arg);
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
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
