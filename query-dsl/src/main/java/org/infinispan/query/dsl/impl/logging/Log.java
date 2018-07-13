package org.infinispan.query.dsl.impl.logging;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * Log abstraction for the query DSL. For this module, message ids ranging from 14801 to 15000 inclusively have been
 * reserved.
 *
 * @author anistor@redhat.com
 * @since 8.2
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends BasicLogger {

   @Message(value = "Argument cannot be null", id = 14801)
   IllegalArgumentException argumentCannotBeNull();

   @Message(value = "'%s' must be an instance of java.lang.Comparable", id = 14802)
   IllegalArgumentException argumentMustBeComparable(String argName);

   @Message(value = "Parameter name cannot be null or empty", id = 14803)
   IllegalArgumentException parameterNameCannotBeNulOrEmpty();

   @Message(value = "Query does not have parameters", id = 14804)
   IllegalStateException queryDoesNotHaveParameters();

   @Message(value = "No parameter named '%s' was found", id = 14805)
   IllegalArgumentException parameterNotFound(String paramName);

   @Message(value = "No parameters named '%s' were found", id = 14806)
   IllegalArgumentException parametersNotFound(String unknownParams);

   @Message(value = "The list of values for 'in(..)' cannot be null or empty", id = 14807)
   IllegalArgumentException listOfValuesForInCannotBeNulOrEmpty();

   @Message(value = "operator was already specified", id = 14808)
   IllegalStateException operatorWasAlreadySpecified();

   @Message(value = "The given condition was created by another factory", id = 14809)
   IllegalArgumentException conditionWasCreatedByAnotherFactory();

   @Message(value = "The given condition is already in use by another builder", id = 14810)
   IllegalArgumentException conditionIsAlreadyInUseByAnotherBuilder();

   @Message(value = "Sentence already started. Cannot use '%s' again.", id = 14811)
   IllegalStateException cannotUseOperatorAgain(String operatorName);

   @Message(value = "%s cannot be null", id = 14812)
   IllegalArgumentException argumentCannotBeNull(String argName);

   @Message(value = "This query already belongs to another query builder", id = 14813)
   IllegalStateException queryAlreadyBelongsToAnotherBuilder();

   @Message(value = "This sub-query does not belong to a parent query builder yet", id = 14814)
   IllegalStateException subQueryDoesNotBelongToAParentQueryBuilder();

   @Message(value = "Grouping cannot be null or empty", id = 14815)
   IllegalArgumentException groupingCannotBeNullOrEmpty();

   @Message(value = "Grouping can be specified only once", id = 14816)
   IllegalStateException groupingCanBeSpecifiedOnlyOnce();

   @Message(value = "Expecting a java.lang.Collection or an array of java.lang.Object", id = 14817)
   IllegalArgumentException expectingCollectionOrArray();

   @Message(value = "Incomplete sentence. Missing attribute path or operator.", id = 14818)
   IllegalStateException incompleteSentence();

   @Message(value = "Cannot visit an incomplete condition.", id = 14819)
   IllegalStateException incompleteCondition();

   @Message(value = "Old child condition not found in parent condition", id = 14820)
   IllegalStateException conditionNotFoundInParent();

   @Message(value = "Projection cannot be null or empty", id = 14821)
   IllegalArgumentException projectionCannotBeNullOrEmpty();

   @Message(value = "Projection can be specified only once", id = 14822)
   IllegalStateException projectionCanBeSpecifiedOnlyOnce();

   @Message(value = "maxResults must be greater than 0", id = 14823)
   IllegalArgumentException maxResultMustBeGreaterThanZero();

   @Message(value = "startOffset cannot be less than 0", id = 14824)
   IllegalArgumentException startOffsetCannotBeLessThanZero();

   @Message(value = "Query parameter '%s' was not set", id = 14825)
   IllegalStateException queryParameterNotSet(String paramName);

   @Message(value = "Left and right condition cannot be the same", id = 14826)
   IllegalArgumentException leftAndRightCannotBeTheSame();
}
