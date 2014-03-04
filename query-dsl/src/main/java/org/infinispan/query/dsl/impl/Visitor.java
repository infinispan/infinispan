package org.infinispan.query.dsl.impl;

import org.infinispan.query.dsl.Query;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
interface Visitor<ReturnType> {

   ReturnType visit(EqOperator operator);

   ReturnType visit(GtOperator operator);

   ReturnType visit(GteOperator operator);

   ReturnType visit(LtOperator operator);

   ReturnType visit(LteOperator operator);

   ReturnType visit(BetweenOperator operator);

   ReturnType visit(LikeOperator operator);

   ReturnType visit(IsNullOperator operator);

   ReturnType visit(InOperator operator);

   ReturnType visit(ContainsOperator operator);

   ReturnType visit(ContainsAllOperator operator);

   ReturnType visit(ContainsAnyOperator operator);

   ReturnType visit(AttributeCondition attributeCondition);

   ReturnType visit(AndCondition booleanCondition);

   ReturnType visit(OrCondition booleanCondition);

   ReturnType visit(NotCondition notCondition);

   <T extends Query> ReturnType visit(BaseQueryBuilder<T> baseQueryBuilder);
}
