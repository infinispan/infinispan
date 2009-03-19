package org.horizon.commands.write;

import org.horizon.commands.DataCommand;

/**
 * Mixes features from DataCommand and WriteCommand
 *
 * @author Manik Surtani
 * @since 1.0
 */
public interface DataWriteCommand extends WriteCommand, DataCommand {
}
