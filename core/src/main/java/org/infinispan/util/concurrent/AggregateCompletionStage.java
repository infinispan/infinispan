package org.infinispan.util.concurrent;

import java.util.concurrent.CompletionStage;

/**
 * Aggregation of multiple {@link CompletionStage} instances where their size is not known or when a large amount
 * of stages are required as it provides less memory foot print per stage.
 * <p>
 * This stage allows for multiple CompletionStages to be registered via {@link #dependsOn(CompletionStage)}. This
 * AggregateCompletionStage will not complete until it is frozen via {@link #freeze()} and all of the registered
 * CompletionStages complete. If one of the stages that is being depended upon completes with an exception
 * this AggregateCompletionStage will complete with the same Throwable cause after all stages are complete.
 */
public interface AggregateCompletionStage<R> {
   /**
    * Adds another CompletionStage for this stage to be reliant upon.
    * <p>
    * If this CombinedCompletionStage is frozen, it will throw an {@link IllegalStateException}
    * @param stage stage to depend on
    * @return this stage
    */
   AggregateCompletionStage<R> dependsOn(CompletionStage<?> stage);

   /**
    * Marks this composed stage as frozen, allowing it to complete when all stages it depends on complete
    * @return this stage
    */
   CompletionStage<R> freeze();
}
