package org.infinispan.commands.write;

import java.util.Collection;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.metadata.impl.PrivateMetadata;

/**
 * A command that modifies the cache in some way
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface WriteCommand extends VisitableCommand, FlagAffectedCommand, TopologyAffectedCommand {
   /**
    * Some commands may want to provide information on whether the command was successful or not.  This is different
    * from a failure, which usually would result in an exception being thrown.  An example is a putIfAbsent() not doing
    * anything because the key in question was present.  This would result in a isSuccessful() call returning false.
    *
    * @return true if the command completed successfully, false otherwise.
    */
   boolean isSuccessful();

   /**
    * Some commands may be successful but not need to be replicated to other nodes, stores or listeners. For example
    * a unconditional remove may be performed on a key that doesn't exist. In that case the command is still successful
    * but does not need to replicate that information other places.
    * @param ctx invocation context if present, may be null
    * @param requireReplicateIfRemote if the command can replicate even if not a locally invoked command
    * @return whether the command should replicate
    * @implSpec default just invokes {@link #isSuccessful()}
    */
   default boolean shouldReplicate(InvocationContext ctx, boolean requireReplicateIfRemote) {
      return isSuccessful();
   }

   /**
    * Certain commands only work based on a certain condition or state of the cache.  For example, {@link
    * org.infinispan.Cache#putIfAbsent(Object, Object)} only does anything if a condition is met, i.e., the entry in
    * question is not already present.  This method tests whether the command in question is conditional or not.
    *
    * @return true if the command is conditional, false otherwise
    */
   boolean isConditional();

   /**
    * @return The current value matching policy.
    */
   ValueMatcher getValueMatcher();

   /**
    * @param valueMatcher The new value matching policy.
    */
   void setValueMatcher(ValueMatcher valueMatcher);

   /**
    *
    * @return a collection of keys affected by this write command.  Some commands - such as ClearCommand - may return
    * an empty collection for this method.
    */
   Collection<?> getAffectedKeys();

   /**
    * Make subsequent invocations of {@link #isSuccessful()} return <code>false</code>.
    */
   void fail();

   /**
    * Indicates whether the command is write-only, meaning that it makes no
    * attempt to read the previously associated value with key for which the
    * command is directed.
    *
    * @return true is the command is write only, false otherwise.
    */
   default boolean isWriteOnly() {
      return false;
   }

   /**
    * @return the {@link CommandInvocationId} associated to the command.
    */
   CommandInvocationId getCommandInvocationId();

   PrivateMetadata getInternalMetadata(Object key);

   void setInternalMetadata(Object key, PrivateMetadata internalMetadata);

}
