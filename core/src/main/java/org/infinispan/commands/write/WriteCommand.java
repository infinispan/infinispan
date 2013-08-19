package org.infinispan.commands.write;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.VisitableCommand;

import java.util.Set;

/**
 * A command that modifies the cache in some way
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface WriteCommand extends VisitableCommand, FlagAffectedCommand {
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
    * Only relevant for conditional commands.
    *
    * @return {@code true} if the command isn't really conditional, because the previous value was already checked
    * - either on the originator (tx) or on the primary owner (non-tx).
    */
   boolean isIgnorePreviousValue();

   /**
    * Only relevant for conditional commands.
    *
    * @param ignorePreviousValue {@code true} if the command isn't really conditional, because the previous value
    * was already checked - either on the originator (tx) or on the primary owner (non-tx).
    */
   void setIgnorePreviousValue(boolean ignorePreviousValue);

   /**
    *
    * @return a collection of keys affected by this write command.  Some commands - such as ClearCommand - may return
    * an empty collection for this method.
    */
   Set<Object> getAffectedKeys();

}
