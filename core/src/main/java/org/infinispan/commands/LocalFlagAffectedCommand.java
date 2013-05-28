package org.infinispan.commands;

import org.infinispan.context.Flag;

import java.util.Set;

/**
 * Commands affected by Flags will be checked locally to control certain behaviors such whether or not to invoke
 * certain commands remotely, check cache store etc.
 *
 * @author William Burns
 * @since 6.0
 */
public interface LocalFlagAffectedCommand {
   /**
    * @return the Flags which where set in the context - only valid to invoke after {@link #setFlags(java.util.Set)}
    */
   Set<Flag> getFlags();

   /**
    * Use it to store the flags from the InvocationContext into the Command before remoting the Command.
    * @param flags
    */
   void setFlags(Set<Flag> flags);

   /**
    * Use it to store the flags from the InvocationContext into the Command before remoting the Command.
    * @param flags
    */
   void setFlags(Flag... flags);

   /**
    * Check whether a particular flag is present in the command
    *
    * @param flag to lookup in the command
    * @return true if the flag is present
    */
   boolean hasFlag(Flag flag);
}
