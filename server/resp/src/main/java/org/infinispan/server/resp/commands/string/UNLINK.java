package org.infinispan.server.resp.commands.string;

/**
 * UNLINK
 * <p>
 * Infinispan has no asynchronous reclamation, so UNLINK behaves exactly like {@link DEL}:
 * it removes the given keys and replies with the number of keys removed.
 *
 * @see <a href="https://redis.io/commands/unlink/">UNLINK</a>
 * @since 16.4
 */
public class UNLINK extends DEL {
}
