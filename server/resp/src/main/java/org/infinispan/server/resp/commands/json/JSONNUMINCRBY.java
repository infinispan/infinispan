package org.infinispan.server.resp.commands.json;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.json.EmbeddedJsonCache;

/**
 * JSON.NUMINCRBY
 *
 * @see <a href="https://redis.io/commands/json.numincrby/">JSON.NUMINCRBY</a>
 * @since 15.2
 */
public class JSONNUMINCRBY extends JSONNUM {
    public JSONNUMINCRBY() {
        super("JSON.NUMINCRBY",  AclCategory.JSON.mask() | AclCategory.WRITE.mask() | AclCategory.SLOW.mask());
    }

    @Override
    CompletionStage<List<Number>> perform(EmbeddedJsonCache ejc, byte[] key, byte[] jsonPath, byte[] value) {
        return ejc.numIncBy(key, jsonPath, value);
    }
}
