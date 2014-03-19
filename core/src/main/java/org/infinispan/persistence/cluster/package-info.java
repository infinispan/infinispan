/**
 * A {@link CacheLoader} (not {@link CacheStore}) that polls other nodes in the cluster for state.  Useful if state
 * transfer on startup is disabled, this {@link CacheLoader} implementation allows for lazily loading state from
 * remote nodes, on demand and on a per-entry basis.
 *
 * @public
 */
package org.infinispan.persistence.cluster;