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

   public Set<Object> getAffectedKeys() {
      return Collections.singleton(key);
   }
}
