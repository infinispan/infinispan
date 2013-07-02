package org.infinispan.commands;

/**
 * This is a marker interface to indicate that such commands will never be replicated and hence will not return any
 * valid command IDs.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface LocalCommand {
}
