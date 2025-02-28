package org.infinispan.commands.read;

import org.infinispan.commands.AbstractFlagAffectedCommand;
import org.infinispan.commands.LocalCommand;

/**
 * Abstract class
 *
 * @author Manik Surtani
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public abstract class AbstractLocalCommand extends AbstractFlagAffectedCommand implements LocalCommand {

   protected AbstractLocalCommand() {
      super(0);
   }

   public boolean isReturnValueExpected() {
      return false;
   }

   @Override
   public LoadType loadType() {
      throw new UnsupportedOperationException();
   }
}
