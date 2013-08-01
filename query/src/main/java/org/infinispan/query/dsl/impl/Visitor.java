package org.infinispan.query.dsl.impl;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
interface Visitor<ResultType> {

   ResultType visit(AttributeCondition attributeCondition);

   ResultType visit(CompositeCondition compositeCondition);

   ResultType visit(LuceneQueryBuilder luceneQueryBuilder);
}
