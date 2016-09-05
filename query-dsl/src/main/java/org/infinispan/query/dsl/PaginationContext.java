package org.infinispan.query.dsl;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
public interface PaginationContext<Context extends PaginationContext> {

   Context startOffset(long startOffset);

   Context maxResults(int maxResults);
}
