package org.infinispan.remoting.rpc;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.lifecycle.ComponentStatus;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;

/**
 * @author anistor@redhat.com
 * @since 5.3
 */
public class CustomCacheRpcCommand extends BaseRpcCommand implements VisitableCommand, Serializable {

   public static final byte COMMAND_ID = 126;

   private static final long serialVersionUID = -1L;

   private Object arg;

   public CustomCacheRpcCommand() {
      super(null); // For command id uniqueness test
   }

   public CustomCacheRpcCommand(String cacheName) {
      super(cacheName);
   }

   public CustomCacheRpcCommand(String cacheName, Object arg) {
      this(cacheName);
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
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeObject(arg);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      arg = input.readObject();
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
   public boolean shouldInvoke(InvocationContext ctx) {
      return true;
   }

   @Override
   public boolean ignoreCommandOnStatus(ComponentStatus status) {
      return false;
   }

   @Override
   public boolean readsExistingValues() {
      return false;
   }
}
