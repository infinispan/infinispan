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

   @Override
   public boolean canBlock() {
      return true;
   }
}
