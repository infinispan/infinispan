package org.infinispan.server.resp.json;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.Proto;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Bucket used to store Set data type.
 *
 * @author Vittorio Rigamonti
 * @since 15.0
 */
@Proto
@ProtoTypeId(ProtoStreamTypeIds.RESP_JSON_BUCKET)
public record JsonBucket(byte[] value) {
}
