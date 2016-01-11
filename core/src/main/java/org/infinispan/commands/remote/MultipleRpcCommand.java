package org.infinispan.commands.remote;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.tx.TransactionBoundaryCommand;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.context.InvocationContext;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.List;

/**
 * Command that implements cluster replication logic.
 * <p/>
 * This is not a {@link VisitableCommand} and hence not passed up the {@link org.infinispan.interceptors.base.CommandInterceptor}
 * chain.
 * <p/>
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class MultipleRpcCommand extends BaseRpcInvokingCommand {

   public static final byte COMMAND_ID = 2;

   private static final Log log = LogFactory.getLog(MultipleRpcCommand.class);
   private static final boolean trace = log.isTraceEnabled();

   private ReplicableCommand[] commands;

   private MultipleRpcCommand() {
      super(null); // For command id uniqueness test
   }

   public MultipleRpcCommand(List<ReplicableCommand> modifications, String cacheName) {
      super(cacheName);
      commands = modifications.toArray(new ReplicableCommand[modifications.size()]);
   }

   public MultipleRpcCommand(String cacheName) {
      super(cacheName);
   }

   /**
    * Executes commands replicated to the current cache instance by other cache instances.
    */
   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      if (trace) log.tracef("Executing remotely originated commands: %d", commands.length);
      for (ReplicableCommand command : commands) {
         if (command instanceof TransactionBoundaryCommand) {
            command.perform(null);
         } else {
            processVisitableCommand(command);
         }
      }
      return null;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   public ReplicableCommand[] getCommands() {
      return commands;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      MarshallUtil.marshallArray(commands, output);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      commands = MarshallUtil.unmarshallArray(input, ReplicableCommand[]::new);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof MultipleRpcCommand)) return false;

      MultipleRpcCommand that = (MultipleRpcCommand) o;

      if (cacheName != null ? !cacheName.equals(that.cacheName) : that.cacheName != null) return false;
      if (!Arrays.equals(commands, that.commands)) return false;
      if (interceptorChain != null ? !interceptorChain.equals(that.interceptorChain) : that.interceptorChain != null)
         return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = interceptorChain != null ? interceptorChain.hashCode() : 0;
      result = 31 * result + (commands != null ? Arrays.hashCode(commands) : 0);
      result = 31 * result + (cacheName != null ? cacheName.hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      return "MultipleRpcCommand{" +
            "commands=" + (commands == null ? null : Arrays.asList(commands)) +
            ", cacheName='" + cacheName + '\'' +
            '}';
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   @Override
   public boolean canBlock() {
      for (ReplicableCommand command : commands) {
         if (command.canBlock()) {
            return true;
         }
      }
      return false;
   }
}
