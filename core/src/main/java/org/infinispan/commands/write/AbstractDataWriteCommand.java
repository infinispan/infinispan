package org.infinispan.commands.write;

import org.infinispan.commands.read.AbstractDataCommand;
import org.infinispan.context.Flag;

import java.util.Collections;
import java.util.Set;

/**
 * Stuff common to WriteCommands
 *
 * @author Manik Surtani
 * @since 4.0
 */
public abstract class AbstractDataWriteCommand extends AbstractDataCommand implements DataWriteCommand {

   protected boolean previousRead;
   
   protected AbstractDataWriteCommand() {
   }

   protected AbstractDataWriteCommand(Object key, Set<Flag> flags) {
      super(key, flags);
   }

   @Override
   public Set<Object> getAffectedKeys() {
      return Collections.singleton(key);
   }

   @Override
   public boolean isReturnValueExpected() {
      return flags == null || (!flags.contains(Flag.SKIP_REMOTE_LOOKUP)
                                  && !flags.contains(Flag.IGNORE_RETURN_VALUES));
   }

   /**
    * It marks the key as read when this write command was executed. This is only used when write skew check is enabled.
    * 
    * @param value   {@code true} if the key was previous read before this command execution
    */
   public final void setPreviousRead(boolean value) {
      this.previousRead = value;
   }
   
   @Override
   public final boolean wasPreviousRead() {
      return previousRead;
   }
   
   @Override
   public boolean canBlock() {
      return true;
   }
}
