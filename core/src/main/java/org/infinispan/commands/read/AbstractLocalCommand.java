package org.infinispan.commands.read;

import java.io.IOException;

import org.infinispan.commands.AbstractFlagAffectedCommand;
import org.infinispan.commands.LocalCommand;
import org.infinispan.commons.marshall.UserObjectInput;
import org.infinispan.marshall.core.MarshalledEntryFactory;
import org.infinispan.commons.marshall.UserObjectOutput;

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

   public final void writeTo(UserObjectOutput output, MarshalledEntryFactory entryFactory) throws IOException {
      //no-op
   }

   public final void readFrom(UserObjectInput input) throws IOException, ClassNotFoundException {
      //no-op
   }

   public boolean isReturnValueExpected() {
      return false;
   }

   public boolean canBlock() {
      return false;
   }
}
