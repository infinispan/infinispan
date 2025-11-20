package org.infinispan.client.rest.impl.jdk.sse;

import static java.util.stream.Collectors.toMap;

import java.io.Closeable;
import java.net.http.HttpHeaders;
import java.net.http.HttpResponse;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Flow;

import org.infinispan.client.rest.RestEventListener;
import org.infinispan.client.rest.impl.jdk.RestResponseInfoJDK;
import org.infinispan.commons.dataconversion.MediaType;

public class EventSubscriber implements Flow.Subscriber<String>, Closeable {

   private Flow.Subscription subscription;
   private final List<String> lines = new ArrayList<>();
   private final RestEventListener listener;
   private RestResponseInfoJDK responseInfo;

   public EventSubscriber(RestEventListener listener) {
      this.listener = listener;
   }

   @Override
   public void onSubscribe(Flow.Subscription subscription) {
      listener.onOpen(responseInfo);
      this.subscription = subscription;
      subscription.request(1);
   }

   @Override
   public void onNext(String line) {
      if (line.isEmpty()) {
         Map<String, String> map = lines.stream()
               .map(l -> l.split(":", 2))
               .filter(pair -> !pair[0].isEmpty())
               .collect(toMap(pair -> pair[0], pair -> pair[1].substring(1), (s1, s2) -> s1 + "\n" + s2));
         listener.onMessage(map.get("id"), map.get("event"), map.get("data"));
         lines.clear();
      } else {
         lines.add(line);
      }
      subscription.request(1);
   }

   @Override
   public void onError(Throwable throwable) {
      if (subscription != null) {
         listener.onError(throwable, responseInfo);
      }
   }

   @Override
   public void onComplete() {
      subscription = null;
      listener.close();
   }

   @Override
   public void close() {
      var sub = subscription;
      subscription = null;
      listener.close();
      if (sub != null) sub.cancel();
   }

   public HttpResponse.BodyHandler<Void> bodyHandler() {
      return (responseInfo) -> {
         this.responseInfo = new RestResponseInfoJDK(responseInfo);
         return HttpResponse.BodySubscribers.fromLineSubscriber(this,
               s -> null,
               charsetFrom(responseInfo.headers()),
               null);
      };
   }

   public static Charset charsetFrom(HttpHeaders headers) {
      String type = headers.firstValue("Content-type").orElse("text/html; charset=utf-8");
      MediaType mediaType = MediaType.parseList(type).findFirst().get();
      return mediaType.getCharset();
   }
}
