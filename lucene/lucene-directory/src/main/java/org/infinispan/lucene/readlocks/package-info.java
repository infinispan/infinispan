/**
 * Several implementations for a SegmentReadLocker, pick one depending on your use case.
 * Lucene's default IndexDeletionPolicy could remove a segment while it's still used by another IndexReader;
 * this is not an issue on a local filesystem, but could happen on Infinispan.
 * To prevent deletion of in-use segments a read-lock is acquired when a segment is opened.
 */
package org.infinispan.lucene.readlocks;
