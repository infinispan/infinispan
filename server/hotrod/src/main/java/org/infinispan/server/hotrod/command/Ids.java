package org.infinispan.server.hotrod.command;

import org.infinispan.commands.ReplicableCommand;

/**
 * The ids of the {@link ReplicableCommand} used by this module.
 * <p>
 * range: 140-141
 *
 * @author Pedro Ruivo
 * @since 9.1
 */
public interface Ids {

   byte FORWARD_COMMIT = (byte) (140 & 0xFF);
   byte FORWARD_ROLLBACK = (byte) (141 & 0xFF);
}
