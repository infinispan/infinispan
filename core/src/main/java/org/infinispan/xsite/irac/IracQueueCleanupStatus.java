package org.infinispan.xsite.irac;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.Proto;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * The status of the queue cleanup in a single node.
 *
 * @param state              One of {@link IracQueueCleanup.State}.
 * @param queueSize          The number of keys queued to be sent to the remote site(s).
 * @param oldestKeyAgeMillis The age of the oldest queued key, or 0 if the queue is empty.
 * @param checked            The number of keys checked by the last or current cleanup.
 * @param candidates         The number of keys without an entry and without a tombstone found in the first check.
 * @param removed            The number of keys removed after the second check.
 * @since 16.4
 */
@Proto
@ProtoTypeId(ProtoStreamTypeIds.IRAC_QUEUE_CLEANUP_STATUS)
public record IracQueueCleanupStatus(String state, int queueSize, long oldestKeyAgeMillis, long checked,
                                     long candidates, long removed) implements JsonSerialization {

   public static final IracQueueCleanupStatus NOT_AVAILABLE = new IracQueueCleanupStatus("NOT_AVAILABLE", 0, 0, 0, 0, 0);

   @Override
   public Json toJson() {
      return Json.object()
            .set("state", state)
            .set("queue_size", queueSize)
            .set("oldest_key_age_ms", oldestKeyAgeMillis)
            .set("checked", checked)
            .set("candidates", candidates)
            .set("removed", removed);
   }
}
