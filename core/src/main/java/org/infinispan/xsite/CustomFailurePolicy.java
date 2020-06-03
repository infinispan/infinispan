package org.infinispan.xsite;

/**
 * Used for implementing custom policies in case of communication failures with a remote site. The handle methods are
 * allowed to throw instances of {@link BackupFailureException} to signal that they want the intra-site operation to
 * fail as well. If handle methods don't throw any exception then the operation will succeed in the local cluster. For
 * convenience, there is a support implementation of this class: {@link AbstractCustomFailurePolicy}
 * <p/>
 * Lifecycle: the same instance is invoked during the lifecycle of a cache so it is allowed to hold state between
 * invocations.
 * <p/>
 * Threadsafety: instances of this class might be invoked from different threads and they should be synchronized.
 *
 * @author Mircea Markus
 * @see BackupFailureException
 * @since 5.2
 * @deprecated since 11.0. Use {@link org.infinispan.configuration.cache.CustomFailurePolicy} instead.
 */
@Deprecated
public interface CustomFailurePolicy<K, V> extends org.infinispan.configuration.cache.CustomFailurePolicy<K, V> {

}
