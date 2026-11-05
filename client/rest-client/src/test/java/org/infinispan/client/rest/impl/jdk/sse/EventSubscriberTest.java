package org.infinispan.client.rest.impl.jdk.sse;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.http.HttpHeaders;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Flow;

import org.infinispan.client.rest.RestEventListener;
import org.junit.jupiter.api.Test;

/**
 * Verifies the server-sent events line parsing performed by {@link EventSubscriber}.
 */
class EventSubscriberTest {

   @Test
   void parsesSseMessage() {
      RecordingListener listener = new RecordingListener();
      try (EventSubscriber subscriber = new EventSubscriber(listener)) {
         subscriber.onSubscribe(new NoOpSubscription());
         subscriber.onNext("id: 1");
         subscriber.onNext("event: ping");
         subscriber.onNext("data: hello");
         subscriber.onNext(""); // the blank line terminates the event
      }
      assertThat(listener.messages).hasSize(1);
      assertThat(listener.messages.get(0).id()).isEqualTo("1");
      assertThat(listener.messages.get(0).type()).isEqualTo("ping");
      assertThat(listener.messages.get(0).data()).isEqualTo("hello");
   }

   @Test
   void multiLineDataIsJoined() {
      RecordingListener listener = new RecordingListener();
      try (EventSubscriber subscriber = new EventSubscriber(listener)) {
         subscriber.onSubscribe(new NoOpSubscription());
         subscriber.onNext("data: line1");
         subscriber.onNext("data: line2");
         subscriber.onNext("");
      }
      assertThat(listener.messages).hasSize(1);
      assertThat(listener.messages.get(0).data()).isEqualTo("line1\nline2");
   }

   @Test
   void commentsAreIgnored() {
      RecordingListener listener = new RecordingListener();
      try (EventSubscriber subscriber = new EventSubscriber(listener)) {
         subscriber.onSubscribe(new NoOpSubscription());
         subscriber.onNext(": a comment");
         subscriber.onNext("data: hello");
         subscriber.onNext("");
      }
      assertThat(listener.messages).hasSize(1);
      assertThat(listener.messages.get(0).data()).isEqualTo("hello");
      assertThat(listener.messages.get(0).id()).isNull();
   }

   @Test
   void closePropagatesToListener() {
      // No try-with-resources here: the explicit close() would close the listener a second time.
      RecordingListener listener = new RecordingListener();
      EventSubscriber subscriber = new EventSubscriber(listener);
      subscriber.onSubscribe(new NoOpSubscription());
      subscriber.onComplete();
      assertThat(listener.closeCount).isEqualTo(1);
   }

   @Test
   void charsetFromHeader() {
      HttpHeaders headers = HttpHeaders.of(Map.of("Content-type", List.of("text/event-stream; charset=utf-8")), (n, v) -> true);
      assertThat(EventSubscriber.charsetFrom(headers)).isEqualTo(StandardCharsets.UTF_8);
   }

   @Test
   void charsetFromDefault() {
      HttpHeaders headers = HttpHeaders.of(Map.of(), (n, v) -> true);
      assertThat(EventSubscriber.charsetFrom(headers)).isEqualTo(StandardCharsets.UTF_8);
   }

   private static class RecordingListener implements RestEventListener {
      final List<Msg> messages = new ArrayList<>();
      int closeCount = 0;

      @Override
      public void onMessage(String id, String type, String data) {
         messages.add(new Msg(id, type, data));
      }

      @Override
      public void close() {
         closeCount++;
      }

      record Msg(String id, String type, String data) {
      }
   }

   private static class NoOpSubscription implements Flow.Subscription {
      @Override
      public void request(long n) {
      }

      @Override
      public void cancel() {
      }
   }
}
