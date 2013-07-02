package org.infinispan.commands;

import java.util.Set;

import org.infinispan.context.Flag;

/**
 * Commands affected by Flags should carry them over to the remote nodes.
 * 
 * By implementing this interface the remote handler will read them out and restore in context;
 * flags should still be evaluated in the InvocationContext.
 * 
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
 * @since 5.0
 */
public interface FlagAffectedCommand extends VisitableCommand, TopologyAffectedCommand, MetadataAwareCommand {
   
   /**
    * @return the Flags which where set in the context - only valid to invoke after {@link #setFlags(Set)}
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
