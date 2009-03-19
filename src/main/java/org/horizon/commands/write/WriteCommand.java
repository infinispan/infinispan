package org.horizon.commands.write;

import org.horizon.commands.VisitableCommand;

/**
 * A command that modifies the cache in some way
 *
 * @author Manik Surtani
 * @since 1.0
 */
public interface WriteCommand extends VisitableCommand {
   /**
    * Some commands may want to provide information on whether the command was successful or not.  This is different
    * from a failure, which usually would result in an exception being thrown.  An example is a putIfAbsent() not doing
    * anything because the key in question was present.  This would result in a isSuccessful() call returning false.
    *
    * @return true if the command completed successfully, false otherwise.
    */
   boolean isSuccessful();
}
