package org.infinispan.server.resp;

import org.infinispan.server.resp.logging.Log;
import org.infinispan.util.logging.LogFactory;

import io.netty.handler.codec.ByteToMessageDecoder;

public abstract class BaseRespDecoder extends ByteToMessageDecoder {
   protected final static Log log = LogFactory.getLog(BaseRespDecoder.class, Log.class);
   protected final Intrinsics.Resp2LongProcessor longProcessor = new Intrinsics.Resp2LongProcessor();
}
