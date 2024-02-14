package org.infinispan.server.resp.commands.string;

/**
 * `<code>SUBSTR key start end</code>` command.
 * <p>
 * This command is deprecated. The recommended alternative is `<code>GETRANGE key start end</code>`.
 * </p>
 *
 * @since 15.0
 * @author José Bolina
 * @see GETRANGE
 * @see <a href="https://redis.io/commands/substr/">Redis documentation</a>.
 */
public class SUBSTR extends GETRANGE { }
