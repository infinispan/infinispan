package org.infinispan.topology;

import org.infinispan.commons.CacheException;

/**
 * Thrown when members are missing after a cluster shutdown.
 *
 * A cluster misses members after the cluster shutdown and a restart, and some previous members did not join again.
 *
 * @since 15.0
 */
public class MissingMembersException extends CacheException {
  public MissingMembersException(String msg) {
    super(msg);
  }
}
