package org.infinispan.commands.write;

import org.infinispan.commands.DataCommand;

/**
 * Mixes features from DataCommand and WriteCommand
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface DataWriteCommand extends WriteCommand, DataCommand {

}
