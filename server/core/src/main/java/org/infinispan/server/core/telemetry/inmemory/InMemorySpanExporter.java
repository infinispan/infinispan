package org.infinispan.server.core.telemetry.inmemory;

import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;

/**
 * Copied and adapted from project https://github.com/open-telemetry/opentelemetry-java
 * Module: testing
 * Class: io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter.java
 * Version: 1.29.0
 * <p>
 * A {@link SpanExporter} implementation that can be used to test OpenTelemetry integration.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * // class MyClassTest {
 * //   private final InMemorySpanExporter testExporter = InMemorySpanExporter.create();
 * //   private final SdkTracerProvider tracerProvider = SdkTracerProvider
 * //     .builder()
 * //     .addSpanProcessor(SimpleSpanProcessor.create(testExporter))
 * //     .build();
 * //
 * //   @Test
 * //   public void getFinishedSpanData() {
 * //     Tracer tracer = tracerProvider.tracerBuilder("test-scope").build();
 * //     tracer.spanBuilder("span").startSpan().end();
 * //
 * //     List<SpanData> spanItems = testExporter.getFinishedSpanItems();
 * //     assertThat(spanItems).isNotNull();
 * //     assertThat(spanItems.size()).isEqualTo(1);
 * //     assertThat(spanItems.get(0).getName()).isEqualTo("span");
 * //   }
 * // }
 * }</pre>
 */
public final class InMemorySpanExporter implements SpanExporter {
   private final Queue<SpanData> finishedSpanItems = new ConcurrentLinkedQueue<>();
   private boolean isStopped = false;

   /**
    * Returns a new instance of the {@code InMemorySpanExporter}.
    *
    * @return a new instance of the {@code InMemorySpanExporter}.
    */
   public static InMemorySpanExporter create() {
      return new InMemorySpanExporter();
   }

   /**
    * Returns a {@code List} of the finished {@code Span}s, represented by {@code SpanData}.
    *
    * @return a {@code List} of the finished {@code Span}s.
    */
   public List<SpanData> getFinishedSpanItems() {
      return List.copyOf(finishedSpanItems);
   }

   /**
    * Clears the internal {@code List} of finished {@code Span}s.
    *
    * <p>Does not reset the state of this exporter if already shutdown.
    */
   public void reset() {
      finishedSpanItems.clear();
   }

   @Override
   public CompletableResultCode export(Collection<SpanData> spans) {
      if (isStopped) {
         return CompletableResultCode.ofFailure();
      }
      finishedSpanItems.addAll(spans);
      return CompletableResultCode.ofSuccess();
   }

   /**
    * The InMemory exporter does not batch spans, so this method will immediately return with
    * success.
    *
    * @return always Success
    */
   @Override
   public CompletableResultCode flush() {
      return CompletableResultCode.ofSuccess();
   }

   /**
    * Clears the internal {@code List} of finished {@code SpanData}s.
    *
    * <p>Any subsequent call to export() function on this SpanExporter, will return {@code
    * CompletableResultCode.ofFailure()}
    */
   @Override
   public CompletableResultCode shutdown() {
      finishedSpanItems.clear();
      isStopped = true;
      return CompletableResultCode.ofSuccess();
   }

   private InMemorySpanExporter() {
   }
}
