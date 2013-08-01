package org.infinispan.query.dsl;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public interface QueryBuilder extends FilterConditionBeginContext {

   //todo sorting might not be supported yet at this stage
   QueryBuilder orderBy(String attributePath, SortOrder sortOrder);

   //todo projection might not be supported yet at this stage
   QueryBuilder setProjection(String... attributePath);

   QueryBuilder startOffset(long startOffset);

   QueryBuilder maxResults(int maxResults);

   String getRootType();

   Query build();
}
