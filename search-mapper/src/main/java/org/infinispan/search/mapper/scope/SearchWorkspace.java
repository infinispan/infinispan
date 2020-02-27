package org.infinispan.search.mapper.scope;

import java.util.Set;

/**
 * The entry point for explicit index operations.
 * <p>
 * A {@link SearchWorkspace} targets a pre-defined set of indexed types (and their indexes).
 */
public interface SearchWorkspace {

   /**
    * Delete all documents from indexes targeted by this workspace.
    * <p>
    * With multi-tenancy enabled, only documents of the current tenant will be removed:
    * the tenant that was targeted by the session from where this workspace originated.
    */
   void purge();

   /**
    * Delete documents from indexes targeted by this workspace
    * that were indexed with any of the given routing keys.
    * <p>
    * With multi-tenancy enabled, only documents of the current tenant will be removed:
    * the tenant that was targeted by the session from where this workspace originated.
    *
    * @param routingKeys The set of routing keys.
    * If non-empty, only documents that were indexed with these routing keys will be deleted.
    * If empty, documents will be deleted regardless of their routing key.
    */
   void purge(Set<String> routingKeys);

   /**
    * Flush to disk the changes to indexes that were not committed yet. In the case of backends with a transaction log
    * (Elasticsearch), also apply operations from the transaction log that were not applied yet.
    * <p>
    * This is generally not useful as Hibernate Search commits changes automatically. Only to be used by experts fully
    * aware of the implications.
    * <p>
    * Note that some operations may still be waiting in a queue when {@link #flush()} is called, in particular
    * operations queued as part of automatic indexing before a transaction is committed. These operations will not be
    * applied immediately just because  a call to {@link #flush()} is issued: the "flush" here is a very low-level
    * operation managed by the backend.
    */
   void flush();

   /**
    * Refresh the indexes so that all changes executed so far will be visible in search queries.
    * <p>
    * This is generally not useful as indexes are refreshed automatically,
    * either after every change (default for the Lucene backend)
    * or periodically (default for the Elasticsearch backend,
    * possible for the Lucene backend by setting a refresh interval).
    * Only to be used by experts fully aware of the implications.
    * <p>
    * Note that some operations may still be waiting in a queue when {@link #refresh()} is called,
    * in particular operations queued as part of automatic indexing before a transaction is committed.
    * These operations will not be applied immediately just because  a call to {@link #refresh()} is issued:
    * the "refresh" here is a very low-level operation handled by the backend.
    */
   void refresh();

   /**
    * Merge all segments of the indexes targeted by this workspace into a single one.
    * <p>
    * Note this operation may affect performance positively as well as negatively. As a rule of thumb, if indexes are
    * read-only for extended periods of time, then calling {@link #mergeSegments()} may improve performance. If indexes
    * are written to, then calling {@link #mergeSegments()} is likely to degrade read/write performance overall.
    */
   void mergeSegments();

}
