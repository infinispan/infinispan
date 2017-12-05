package org.infinispan.query.dsl;

/**
 * Factory for query DSL objects. Query construction starts here, usually by invoking the {@link #from} method which
 * returns a {@link QueryBuilder} capable of constructing {@link Query} objects. The other methods are use for creating
 * sub-conditions.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
public interface QueryFactory {

   /**
    * Creates a Query based on an Ickle query string.
    *
    * @return a query
    */
   Query create(String queryString);

   /**
    * Creates a Query based on an Ickle query string
    * @param queryMode the {@link IndexedQueryMode} dictating the indexed query execution mode if applicable.
    */
   Query create(String queryString, IndexedQueryMode queryMode);

   /**
    * Creates a QueryBuilder for the given entity type.
    *
    * @param entityType the Class of the entity
    * @return a builder capable of creating queries for the specified entity type
    */
   QueryBuilder from(Class<?> entityType);

   /**
    * Creates a QueryBuilder for the given entity type.
    *
    * @param entityType fully qualified entity type name
    * @return a builder capable of creating queries for the specified entity type
    */
   QueryBuilder from(String entityType);

   /**
    * Creates a condition on the given attribute path that is to be completed later by using it as a sub-condition.
    *
    * @param expression a path Expression
    * @return the incomplete sub-condition
    */
   FilterConditionEndContext having(Expression expression);

   /**
    * Creates a condition on the given attribute path that is to be completed later by using it as a sub-condition.
    *
    * @param attributePath the attribute path
    * @return the incomplete sub-condition
    */
   FilterConditionEndContext having(String attributePath);

   /**
    * Creates a negated condition that is to be completed later by using it as a sub-condition.
    *
    * @return the incomplete sub-condition
    */
   FilterConditionBeginContext not();

   /**
    * Creates a negated condition based on a given sub-condition. The negation is grouped.
    *
    * @return the incomplete sub-condition
    */
   FilterConditionContext not(FilterConditionContext fcc);
}
