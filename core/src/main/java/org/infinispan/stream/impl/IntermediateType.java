package org.infinispan.stream.impl;

import java.util.Spliterator;
import java.util.stream.BaseStream;
import java.util.stream.StreamSupport;

import org.infinispan.BaseCacheStream;
import org.infinispan.CacheStream;
import org.infinispan.DoubleCacheStream;
import org.infinispan.IntCacheStream;
import org.infinispan.LongCacheStream;

/**
 * The various intermediate types.  Note that the local intermediate operation can be a type other than the
 * resulting stream, so we have to keep track of what it was to avoid {@link ClassCastException} issues.
 */
public enum IntermediateType {
   REF {
      @Override
      public <T, S extends BaseStream<T, S>> S handleStream(BaseCacheStream streamable) {
         CacheStream<?> stream = (CacheStream<?>) streamable;
         Spliterator<?> spliterator = stream.spliterator();
         return (S) StreamSupport.stream(spliterator, streamable.isParallel());
      }
   },
   INT {
      @Override
      public <T, S extends BaseStream<T, S>> S handleStream(BaseCacheStream streamable) {
         IntCacheStream stream = (IntCacheStream) streamable;
         Spliterator.OfInt spliterator = stream.spliterator();
         return (S) StreamSupport.intStream(spliterator, streamable.isParallel());
      }
   },
   LONG {
      @Override
      public <T, S extends BaseStream<T, S>> S handleStream(BaseCacheStream streamable) {
         LongCacheStream stream = (LongCacheStream) streamable;
         Spliterator.OfLong spliterator = stream.spliterator();
         return (S) StreamSupport.longStream(spliterator, streamable.isParallel());
      }
   },
   DOUBLE {
      @Override
      public <T, S extends BaseStream<T, S>> S handleStream(BaseCacheStream streamable) {
         DoubleCacheStream stream = (DoubleCacheStream) streamable;
         Spliterator.OfDouble spliterator = stream.spliterator();
         return (S) StreamSupport.doubleStream(spliterator, streamable.isParallel());
      }
   };

   public abstract <T, S extends BaseStream<T, S>> S handleStream(BaseCacheStream streamable);
}
