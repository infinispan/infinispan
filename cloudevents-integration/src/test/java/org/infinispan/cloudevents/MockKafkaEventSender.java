package org.infinispan.cloudevents;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.infinispan.cloudevents.impl.KafkaEventSender;
import org.infinispan.cloudevents.impl.Log;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.logging.LogFactory;

public class MockKafkaEventSender implements KafkaEventSender {
   private static final Log log = LogFactory.getLog(MockKafkaEventSender.class, Log.class);
   MockProducer<byte[], byte[]> producer = new MockProducer<>();

   @Override
   public CompletionStage<Void> send(ProducerRecord<byte[], byte[]> record) {
      CompletableFuture<Void> cf = new CompletableFuture<>();
      producer.send(record, (metadata, exception) -> {
         if (exception != null) {
            cf.completeExceptionally(exception);
         } else {
            cf.complete(null);
         }
      });
      return cf;
   }

   public MockProducer<byte[], byte[]> getProducer() {
      return producer;
   }

   void clear() {
      producer.clear();
   }

   public void completeSend() {
      long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      while (!producer.completeNext()) {
         if (System.nanoTime() - deadlineNanos > 0) {
            throw new TimeoutException();
         }
         TestingUtil.sleepThread(10);
      }
      log.tracef("Completed send %s", getProducer().history().get(getProducer().history().size() - 1).key());
   }

   public void completeSend(int count) {
      for (int i = 0; i < count; i++) {
         completeSend();
      }
   }
}
