package org.infinispan.test.concurrent;

import org.infinispan.commands.ReplicableCommand;

/**
 * Matches {@link ReplicableCommand}s.
 *
 * @author Dan Berindei
 * @since 7.0
 */
public interface CommandMatcher {
   boolean accept(ReplicableCommand command);
}
