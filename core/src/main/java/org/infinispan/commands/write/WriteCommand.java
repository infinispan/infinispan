package org.infinispan.commands.write;

import java.util.Collection;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.VisitableCommand;

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
    * Used for conditional commands, to update the status of the command on the originator
    * based on the result of its execution on the primary owner.
    *
    * @deprecated since 9.1
    */
   @Deprecated
   default void updateStatusFromRemoteResponse(Object remoteResponse) {}

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

}
