/**
 * This package contains the implementation of the AffinityIndexManager, that maintains an index divided into shards
 * with storage using the Infinispan Lucene directory. Each index shard is associated with one or more Infinispan
 * segments.
 *
 * @deprecated The Affinity Index Manager is deprecated and will be removed in the next major version. Please use
 * non-shared indexes with file based storage.
 */
package org.infinispan.query.affinity;
