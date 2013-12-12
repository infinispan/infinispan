package org.infinispan.query.dsl;

import java.util.Collection;

/**
 * The context that ends a condition. Here we are expected to specify the right hand side of the filter condition, the
 * operator and the operand, in order to complete the filter.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
public interface FilterConditionEndContext {

   /**
    * Checks that the left operand is equal to one of the (fixed) list of values given as argument.
    *
    * @param values the list of values
    * @return the completed context
    */
   FilterConditionContext in(Object... values);

   /**
    * Checks that the left operand is equal to one of the elements from the Collection of values given as argument.
    *
    * @param values the collection of values
    * @return the completed context
    */
   FilterConditionContext in(Collection values);

   /**
    * Checks that the left argument (which is expected to be a String) matches a wildcard pattern that follows the JPA
    * rules.
    *
    * @param pattern the wildcard pattern
    * @return the completed context
    */
   FilterConditionContext like(String pattern);

   /**
    * Checks that the left argument (which is expected to be an array or a Collection) contains the given element.
    *
    * @param value the value to check
    * @return the completed context
    */
   FilterConditionContext contains(Object value);

   /**
    * Checks that the left argument (which is expected to be an array or a Collection) contains all of the the given
    * elements, in any order.
    *
    * @param values the list of elements to check
    * @return the completed context
    */
   FilterConditionContext containsAll(Object... values);

   /**
    * Checks that the left argument (which is expected to be an array or a Collection) contains all the elements of the
    * given collection, in any order.
    *
    * @param values the Collection of elements to check
    * @return the completed context
    */
   FilterConditionContext containsAll(Collection values);

   /**
    * Checks that the left argument (which is expected to be an array or a Collection) contains any of the the given
    * elements.
    *
    * @param values the list of elements to check
    * @return the completed context
    */
   FilterConditionContext containsAny(Object... values);

   /**
    * Checks that the left argument (which is expected to be an array or a Collection) contains any of the elements of
    * the given collection.
    *
    * @param values the Collection of elements to check
    * @return the completed context
    */
   FilterConditionContext containsAny(Collection values);

   /**
    * Checks that the left argument is null.
    *
    * @return the completed context
    */
   FilterConditionContext isNull();

   /**
    * Checks that the left argument is equal to the given value.
    *
    * @param value the value to compare with
    * @return the completed context
    */
   FilterConditionContext eq(Object value);

   /**
    * Checks that the left argument is less than the given value.
    *
    * @param value the value to compare with
    * @return the completed context
    */
   FilterConditionContext lt(Object value);

   /**
    * Checks that the left argument is less than or equal to the given value.
    *
    * @param value the value to compare with
    * @return the completed context
    */
   FilterConditionContext lte(Object value);

   /**
    * Checks that the left argument is greater than the given value.
    *
    * @param value the value to compare with
    * @return the completed context
    */
   FilterConditionContext gt(Object value);

   /**
    * Checks that the left argument is greater than or equal to the given value.
    *
    * @param value the value to compare with
    * @return the completed context
    */
   FilterConditionContext gte(Object value);

   /**
    * Checks that the left argument is between the given range limits. The limits are inclusive by default, but this can
    * be changed using the methods from the returned {@link RangeConditionContext}
    *
    * @param from the start of the range
    * @param to   the end of the range
    * @return the RangeConditionContext context
    */
   RangeConditionContext between(Object from, Object to);
}
