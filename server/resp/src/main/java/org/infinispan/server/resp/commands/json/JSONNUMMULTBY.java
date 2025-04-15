package org.infinispan.server.resp.commands.json;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.json.EmbeddedJsonCache;

/**
 * JSON.NUMMULTBY
 *
 * @see <a href="https://redis.io/commands/json.nummultby/">JSON.NUMMULTBY</a>
 * @since 15.2
 */
public class JSONNUMMULTBY extends JSONNUM {
    public JSONNUMMULTBY() {
        super("JSON.NUMMULTBY",AclCategory.JSON.mask() | AclCategory.WRITE.mask() | AclCategory.SLOW.mask());
    }

    @Override
    CompletionStage<List<Number>> perform(EmbeddedJsonCache ejc, byte[] key, byte[] jsonPath, byte[] value) {
        return ejc.numMultBy(key, jsonPath, value);
    }
}
