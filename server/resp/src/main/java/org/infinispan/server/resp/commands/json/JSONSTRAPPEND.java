package org.infinispan.server.resp.commands.json;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.json.AppendType;
import org.infinispan.server.resp.json.EmbeddedJsonCache;

/**
 * JSON.STRAPPEND
 *
 * @see <a href="https://redis.io/commands/json.strappend/">JSON.STRAPPEND</a>
 * @since 15.2
 */
public class JSONSTRAPPEND extends JSONAPPEND {
    public JSONSTRAPPEND() {
        super("JSON.STRAPPEND", -3, 1, 1, 1);
    }

    @Override
    protected CompletionStage<List<Long>> performAppend(Resp3Handler handler, byte[] key, byte[] jsonPath,
                                                        byte[] value) {
        EmbeddedJsonCache ejc = handler.getJsonCache();
        return ejc.strAppend(key, jsonPath, value, AppendType.STRING);
    }

    @Override
    protected String getOpType() {
        return "string";
    }

}
