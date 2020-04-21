package org.infinispan.reactive;

import java.util.Map;

import org.infinispan.commons.util.Util;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.functions.Function;

/**
 * Static factory class that provides methods to obtain commonly used instances for interoperation between RxJava
 * and standard JRE.
 * @author wburns
 * @since 10.0
 */
public class RxJavaInterop {
   private RxJavaInterop() { }

   /**
    * Provides a {@link Function} that can be used to convert from an instance of {@link java.util.Map.Entry} to
    * the key of the entry. This is useful for the instance passed to a method like {@link Flowable#map(Function)}.
    * @param <K> key type
    * @param <V> value type
    * @return rxjava function to convert from a Map.Entry to its key.
    */
   public static <K, V> Function<Map.Entry<K, V>, K> entryToKeyFunction() {
      return (Function) entryToKeyFunction;
   }

   public static <R> Function<? super Throwable, Publisher<R>> cacheExceptionWrapper() {
      return (Function) wrapThrowable;
   }

   private static final Function<Map.Entry<Object, Object>, Object> entryToKeyFunction = Map.Entry::getKey;

   private static final Function<? super Throwable, Publisher<?>> wrapThrowable = t -> Flowable.error(Util.rewrapAsCacheException(t));
}
