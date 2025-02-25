package org.infinispan.server.resp.commands.json;

/**
 * JSON.NUMMULTBY
 *
 * @see <a href="https://redis.io/commands/json.nummultby/">JSON.NUMMULTBY</a>
 * @since 15.2
 */
public class JSONNUMMULTBY extends JSONNUM {
    public JSONNUMMULTBY() {
        super("JSON.NUMMULTBY");
    }
}
