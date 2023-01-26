package org.infinispan.util.concurrent;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Delayed;

/**
 * A scheduled CompletionStage that may be cancelled if it has not been started yet.
 * If it is cancelled the stage will be completed exceptionally with a {@link java.util.concurrent.CancellationException}
 *
 * @param <V> The result type
 */
public interface ScheduledCompletableStage<V> extends Delayed, CompletionStage<V> {
}
