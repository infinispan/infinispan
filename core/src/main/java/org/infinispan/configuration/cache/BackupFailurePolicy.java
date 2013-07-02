package org.infinispan.configuration.cache;

/**
 * Defines the possible behaviour in case of failure during x-site.
 *
 * @author Mircea Markus
 * @since 5.2
 */
public enum BackupFailurePolicy {
   IGNORE, WARN, FAIL, CUSTOM
}
