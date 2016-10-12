package org.infinispan.commands.read;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.infinispan.commands.AbstractFlagAffectedCommand;
import org.infinispan.commands.LocalCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.lifecycle.ComponentStatus;

/**
 * Abstract class
 *
 * @author Manik Surtani
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public abstract class AbstractLocalCommand extends AbstractFlagAffectedCommand implements LocalCommand {

   public byte getCommandId() {
      return 0;  // no-op
   }

   public final void writeTo(ObjectOutput output) throws IOException {
      //no-op
   }

   public final void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      //no-op
   }

   public boolean shouldInvoke(InvocationContext ctx) {
      return false;
   }

   public boolean ignoreCommandOnStatus(ComponentStatus status) {
      return false;
   }

   public boolean isReturnValueExpected() {
      return false;
   }

   public boolean canBlock() {
      return false;
   }
}
