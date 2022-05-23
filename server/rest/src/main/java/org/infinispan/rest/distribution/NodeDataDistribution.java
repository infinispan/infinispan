package org.infinispan.rest.distribution;

import java.util.List;

/**
 * Identifies a specific node on a data distribution response.
 *
 * @since 14.0
 */
public interface NodeDataDistribution {

   String name();

   List<String> addresses();
}
