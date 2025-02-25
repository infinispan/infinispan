package org.infinispan.server.resp.commands.json;

/**
 * JSON.NUMINCRBY
 *
 * @see <a href="https://redis.io/commands/json.numincrby/">JSON.NUMINCRBY</a>
 * @since 15.2
 */
public class JSONNUMINCRBY extends JSONNUM {
    public JSONNUMINCRBY() {
        super("JSON.NUMINCRBY");
    }
}
