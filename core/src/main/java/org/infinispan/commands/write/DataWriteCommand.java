package org.infinispan.commands.write;

import org.infinispan.commands.DataCommand;

/**
 * Mixes features from DataCommand and WriteCommand
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface DataWriteCommand extends WriteCommand, DataCommand {
   /**
    * Initializes the {@link BackupWriteRpcCommand} to send the update to backup owner of a key.
    * <p>
    * This method will be invoked in the primary owner only.
    *
    * @param command the {@link BackupWriteRpcCommand} to initialize.
    */
   default void initBackupWriteRpcCommand(BackupWriteRpcCommand command) {
      throw new UnsupportedOperationException();
   }

}
