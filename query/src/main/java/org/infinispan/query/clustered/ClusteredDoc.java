package org.infinispan.query.clustered;

import java.util.UUID;

/**
 * ClusteredDoc.
 *
 * Interface to encapsulate a score doc of a distributed query. NodeUUID it's the node that has the
 * value. And getIndex must return the index of the scoreDoc.
 *
 * @author Israel Lacerra <israeldl@gmail.com>
 * @since 5.1
 */
public interface ClusteredDoc {

   UUID getNodeUuid();

   int getIndex();

}
