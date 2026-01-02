package org.infinispan.server.core.telemetry.jfr;

import static org.infinispan.telemetry.InfinispanSpanAttributes.CACHE_NAME;
import static org.infinispan.telemetry.InfinispanSpanAttributes.CATEGORY;
import static org.infinispan.telemetry.InfinispanSpanAttributes.SERVER_ADDRESS;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.infinispan.commons.util.ByRef;

import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.ReadWriteSpan;
import io.opentelemetry.sdk.trace.ReadableSpan;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.data.EventData;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.data.StatusData;
import io.opentelemetry.sdk.trace.internal.data.ExceptionEventData;
import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.StackTrace;

/**
 * Hook to generate {@link Event}s based on {@link io.opentelemetry.api.trace.Span}.
 * <p>
 * This implementation hooks into OpenTelemetry to generate internal events to record with JFR. This approach enables us
 * to keep track of telemetry and tracing with a lower overhead and easy-to-configure alternative.
 * </p>
 *
 * <p>
 * Although the exporter is active, the JVM must be properly configured to record events with JFR. Otherwise, no event
 * is recorded. The application starts with the `<code>-XX:StartFlightRecording=...</code>` option or during runtime with
 * the `<code>jcmd &lt;PID&gt; JFR.start ...</code>` command line tool.
 * </p>
 *
 * This implementation is an adaptation of
 * <a href="https://github.com/open-telemetry/opentelemetry-java-contrib/tree/main/jfr-events">OpenTelemetry Java Flight Recorder (JFR) Events</a>.
 *
 * @since 15.1.0
 */
public final class JfrSpanProcessor implements SpanProcessor {

   private final Map<SpanContext, JfrSpan> events = new ConcurrentHashMap<>();
   private volatile boolean closed;

   @Override
   public void onStart(Context parentContext, ReadWriteSpan span) {
      if (closed || !span.getSpanContext().isValid()) return;

      JfrSpan event = JfrSpan.from(span);
      if (event.isEnabled()) {
         event.begin();
         events.put(span.getSpanContext(), event);
      }
   }

   @Override
   public boolean isStartRequired() {
      return true;
   }

   @Override
   public void onEnd(ReadableSpan span) {
      JfrSpan event = events.remove(span.getSpanContext());
      if (closed || event == null || !event.shouldCommit()) return;

      event.updateStatus(span.toSpanData());
      event.end();
      event.commit();
   }

   @Override
   public boolean isEndRequired() {
      return true;
   }

   @Override
   public CompletableResultCode shutdown() {
      closed = true;
      events.clear();
      return CompletableResultCode.ofSuccess();
   }

   @Label("JFR Span")
   @Description("Infinispan Tracing Events")
   @Category("infinispan-jfr-span")
   @StackTrace(value = false)
   private static final class JfrSpan extends Event {

      public final String node;
      public final String category;
      public final String cache;
      public final String operation;
      public final String trace;
      public final String span;
      public final String parent;
      public final String kind;
      public String status;

      @Label("status description")
      public String statusDescription;

      public String throwable;


      private JfrSpan(String node, String category, String cache, String operation, String trace, String span,
                      String parent, String kind) {
         this.node = node;
         this.category = category;
         this.cache = cache;
         this.operation = operation;
         this.trace = trace;
         this.span = span;
         this.parent = parent;
         this.kind = kind;
      }

      public static JfrSpan from(ReadableSpan span) {
         // FIXME: Once version 1.38.0+ is allowed, remove this allocation.
         SpanData sd = span.toSpanData();
         ByRef<String> node = new ByRef<>(null), category = new ByRef<>(null), cache = new ByRef<>(null);
         sd.getAttributes().forEach((attr, value) -> {
            switch (attr.getKey()) {
               case SERVER_ADDRESS -> node.set(String.valueOf(value));
               case CATEGORY -> category.set(String.valueOf(value));
               case CACHE_NAME -> cache.set(String.valueOf(value));
            }
         });
         SpanContext ctx = span.getSpanContext();
         return new JfrSpan(node.get(), category.get(), cache.get(), span.getName(), ctx.getTraceId(), ctx.getSpanId(),
               span.getParentSpanContext().getSpanId(), span.getKind().name());
      }

      public void updateStatus(SpanData span) {
         StatusData spanStatus = span.getStatus();
         for (EventData event : span.getEvents()) {
            if (event instanceof ExceptionEventData eed) {
               try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                  PrintStream ps = new PrintStream(baos);
                  eed.getException().printStackTrace(ps);
                  throwable = baos.toString();
                  break;
               } catch (IOException ignore) { }
            }
         }
         this.status = spanStatus.getStatusCode().name();
         this.statusDescription = spanStatus.getDescription();
      }
   }
}
