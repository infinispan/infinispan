package org.infinispan.server.resp.commands.string;

/**
 * SUBSTR
 * <p>
 * This command is deprecated. The recommended alternative is `<code>GETRANGE key start end</code>`.
 * </p>
 *
 * @author José Bolina
 * @see <a href="https://redis.io/commands/substr/">SUBSTR</a>
 * @see GETRANGE
 * @since 15.0
 */
public class SUBSTR extends GETRANGE { }
