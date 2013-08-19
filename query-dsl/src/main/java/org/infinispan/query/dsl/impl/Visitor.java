package org.infinispan.query.dsl.impl;

import org.infinispan.query.dsl.Query;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
interface Visitor<ResultType> {

   ResultType visit(EqOperator operator);

   ResultType visit(GtOperator operator);

   ResultType visit(GteOperator operator);

   ResultType visit(LtOperator operator);

   ResultType visit(LteOperator operator);

   ResultType visit(BetweenOperator operator);

   ResultType visit(LikeOperator operator);

   ResultType visit(IsNullOperator operator);

   ResultType visit(InOperator operator);

   ResultType visit(ContainsOperator operator);

   ResultType visit(ContainsAllOperator operator);

   ResultType visit(ContainsAnyOperator operator);

   ResultType visit(AttributeCondition attributeCondition);

   ResultType visit(AndCondition booleanCondition);

   ResultType visit(OrCondition booleanCondition);

   ResultType visit(NotCondition notCondition);

   <T extends Query> ResultType visit(BaseQueryBuilder<T> baseQueryBuilder);
}
