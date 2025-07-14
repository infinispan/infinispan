package org.infinispan.server.hotrod.streaming;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.time.TimeServiceTicker;
import org.infinispan.commons.util.ByRef;
import org.infinispan.metadata.Metadata;
import org.infinispan.server.core.logging.Log;
import org.infinispan.util.concurrent.WithinThreadExecutor;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

public class StreamingManager {
   private static final Log log = LogFactory.getLog(StreamingManager.class, Log.class);
   private static final AtomicInteger globalIterationId = new AtomicInteger();

   private final Map<Integer, StreamingState> iterationStateMap;

   public StreamingManager(TimeService timeService) {
      Caffeine<Object, Object> builder = Caffeine.newBuilder();
      builder.expireAfterAccess(5, TimeUnit.MINUTES).removalListener(
            (RemovalListener<Integer, StreamingState>) (key, value, cause) -> {
               if (cause.wasEvicted()) {
                  // Keys and values cannot be null as we don't use weak key or values
                  assert key != null;
                  assert value != null;
                  log.removedUnclosedIterator(key.toString());
                  value.close();
               }
            }).ticker(new TimeServiceTicker(timeService)).executor(new WithinThreadExecutor());
      iterationStateMap = builder.<Integer, StreamingState>build().asMap();
   }

   public GetStreamResponse startGetStream(byte[] key, byte[] value, Channel channel, int batchSize) {
      GetStreamingState state = new GetStreamingState(key, channel, value, batchSize);
      int id = globalIterationId.getAndIncrement();
      iterationStateMap.put(id, state);
      return new GetStreamResponse(id, state.nextGet(), state.isGetComplete());
   }

   public GetStreamResponse nextGetStream(Integer streamId) {
      StreamingState state = iterationStateMap.get(streamId);
      if (state == null) {
         return null;
      }
      return new GetStreamResponse(streamId, state.nextGet(), state.isGetComplete());
   }

   public void closeGetStream(Integer iterationId) {
      iterationStateMap.computeIfPresent(iterationId, (k, v) -> {
         v.closeGet();
         return null;
      });
   }

   public int startPutStream(byte[] key, Channel channel, Metadata.Builder metadata, long version) {
      StreamingState state = new PutStreamingState(key, channel, metadata, version);
      int id = globalIterationId.getAndIncrement();
      iterationStateMap.put(id, state);
      return id;
   }

   public StreamingState nextPutStream(Integer streamId, boolean lastChunk, ByteBuf buf) {
      if (lastChunk) {
         ByRef<StreamingState> ref = new ByRef<>(null);
         // We don't use remove, just in case the nextPut throws an exception
         iterationStateMap.computeIfPresent(streamId, (k, v) -> {
            v.nextPut(buf);
            ref.set(v);
            v.closePut();
            return null;
         });

         return ref.get();
      }
      StreamingState state = iterationStateMap.get(streamId);
      if (state == null) {
         return null;
      }
      state.nextPut(buf);
      return state;
   }

   public void closePutStream(Integer streamId) {
      iterationStateMap.computeIfPresent(streamId, (k, v) -> {
         v.closePut();
         return null;
      });
   }
}
