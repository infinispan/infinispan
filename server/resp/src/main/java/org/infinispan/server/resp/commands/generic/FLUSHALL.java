package org.infinispan.server.resp.commands.generic;

/**
 * <a href="https://redis.io/commands/flushall/">FLUSHALL</a>
 *
 * Currently, it invokes FLUSHDB as Infinispan doesn't support multiple Redis databases yet
 *
 * @since 15.0
 */
public class FLUSHALL extends FLUSHDB {
}
