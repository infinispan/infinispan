/**
 * A {@link org.infinispan.persistence.spi.CacheLoader} (not {@link org.infinispan.persistence.spi.CacheWriter}) that polls other nodes in the cluster for state.  Useful if state
 * transfer on startup is disabled, this {@link org.infinispan.persistence.spi.CacheLoader} implementation allows for lazily loading state from
 * remote nodes, on demand and on a per-entry basis.
 *
 * @public
 */
package org.infinispan.persistence.cluster;