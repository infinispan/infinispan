package org.infinispan.commands.write;

import org.infinispan.commands.read.AbstractDataCommand;

import java.util.Collection;
import java.util.Collections;

/**
 * Stuff common to WriteCommands
 *
 * @author Manik Surtani
 * @since 4.0
 */
public abstract class AbstractDataWriteCommand extends AbstractDataCommand implements DataWriteCommand {

   protected AbstractDataWriteCommand() {
   }

   protected AbstractDataWriteCommand(Object key) {
      super(key);
   }

   public Collection<Object> getAffectedKeys() {
      return Collections.singleton(key);
   }
}
