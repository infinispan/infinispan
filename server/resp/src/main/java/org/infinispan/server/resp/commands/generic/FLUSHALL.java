package org.infinispan.server.resp.commands.generic;

/**
 * FLUSHALL
 * <p>
 * Currently, it invokes FLUSHDB as Infinispan doesn't support multiple Redis databases yet
 * </p>
 *
 * @see <a href="https://redis.io/commands/flushall/">FLUSHALL</a>
 * @since 15.0
 */
public class FLUSHALL extends FLUSHDB {
}
