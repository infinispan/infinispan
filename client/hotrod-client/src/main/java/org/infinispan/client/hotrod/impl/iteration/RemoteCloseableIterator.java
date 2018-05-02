package org.infinispan.client.hotrod.impl.iteration;

import static org.infinispan.client.hotrod.impl.Util.await;

import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.exceptions.RemoteIllegalLifecycleStateException;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.impl.operations.IterationEndResponse;
import org.infinispan.client.hotrod.impl.operations.IterationNextOperation;
import org.infinispan.client.hotrod.impl.operations.IterationNextResponse;
import org.infinispan.client.hotrod.impl.operations.IterationStartOperation;
import org.infinispan.client.hotrod.impl.operations.IterationStartResponse;
import org.infinispan.client.hotrod.impl.operations.OperationsFactory;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.CloseableIterator;

import io.netty.channel.Channel;
import net.jcip.annotations.NotThreadSafe;

/**
 * @author gustavonalle
 * @since 8.0
 */
@NotThreadSafe
public class RemoteCloseableIterator<E> implements CloseableIterator<Entry<Object, E>> {

   private static final Log log = LogFactory.getLog(RemoteCloseableIterator.class);

   private final OperationsFactory operationsFactory;
   protected final Marshaller marshaller;
   private final String filterConverterFactory;
   private final byte[][] filterParams;
   private final Set<Integer> segments;
   private final int batchSize;
   private final boolean metadata;
   private final DataFormat dataFormat;

   private KeyTracker segmentKeyTracker;
   private Channel channel;
   private byte[] iterationId;
   private boolean endOfIteration = false;
   private boolean closed;
   private Queue<Entry<Object, E>> nextElements = new LinkedList<>();

   public RemoteCloseableIterator(OperationsFactory operationsFactory, Marshaller marshaller, String filterConverterFactory,
                                  byte[][] filterParams, Set<Integer> segments, int batchSize, boolean metadata, DataFormat dataFormat) {
      this.marshaller = marshaller;
      this.filterConverterFactory = filterConverterFactory;
      this.filterParams = filterParams;
      this.segments = segments;
      this.batchSize = batchSize;
      this.operationsFactory = operationsFactory;
      this.metadata = metadata;
      this.dataFormat = dataFormat;
   }

   public RemoteCloseableIterator(OperationsFactory operationsFactory, Marshaller marshaller, int batchSize, Set<Integer> segments, boolean metadata, DataFormat dataFormat) {
      this(operationsFactory, marshaller, null, null, segments, batchSize, metadata, dataFormat);
   }

   @Override
   public void close() {
      if (!closed) {
         try {
            IterationEndResponse endResponse = await(operationsFactory.newIterationEndOperation(iterationId, channel).execute());
            short status = endResponse.getStatus();

            if (HotRodConstants.isSuccess(status) && log.isDebugEnabled()) {
               log.iterationClosed(iterationId());
            }
            if (HotRodConstants.isInvalidIteration(status)) {
               throw log.errorClosingIteration(iterationId());
            }
         } catch (HotRodClientException e) {
            log.ignoringErrorDuringIterationClose(iterationId(), e);
         } finally {
            closed = true;
         }
      }
   }

   private String iterationId() {
      return new String(iterationId, HotRodConstants.HOTROD_STRING_CHARSET);
   }

   @Override
   public boolean hasNext() {
      if (!endOfIteration && nextElements.isEmpty()) {
         fetch();
      }
      return !endOfIteration;
   }

   @Override
   public Entry<Object, E> next() {
      if (!hasNext()) throw new NoSuchElementException();
      return nextElements.remove();
   }

   private void fetch() {
      // We must not execute sync operation in event loop
      assert !channel.eventLoop().inEventLoop();

      try {
         while (nextElements.isEmpty() && !endOfIteration) {
            IterationNextOperation<E> iterationNextOperation = operationsFactory.newIterationNextOperation(iterationId, channel, segmentKeyTracker, dataFormat);
            IterationNextResponse<E> iterationNextResponse = await(iterationNextOperation.execute());
            if (!iterationNextResponse.hasMore()) {
               endOfIteration = true;
               // May as well close out iterator early. This way iterator is always closed when fully iterating upon
               // lowering chance for user to leave it open.
               close();
               break;
            }
            nextElements.addAll(iterationNextResponse.getEntries());
         }

      } catch (TransportException | RemoteIllegalLifecycleStateException e) {
         log.warnf(e, "Error reaching the server during iteration");
         startInternal(segmentKeyTracker.missedSegments());
         fetch();
      }
   }

   private IterationStartResponse startInternal(Set<Integer> segments) {
      if (log.isDebugEnabled()) {
         log.debugf("Starting iteration with segments %s", segments);
      }
      IterationStartOperation iterationStartOperation = operationsFactory.newIterationStartOperation(filterConverterFactory, filterParams, segments, batchSize, metadata, dataFormat);
      IterationStartResponse startResponse = await(iterationStartOperation.execute());
      this.channel = startResponse.getChannel();
      this.iterationId = startResponse.getIterationId();
      if (log.isDebugEnabled()) {
         log.iterationTransportObtained(channel.remoteAddress(), iterationId());
         log.startedIteration(iterationId());
      }
      return startResponse;
   }

   public void start() {
      IterationStartResponse startResponse = startInternal(segments);
      this.segmentKeyTracker = KeyTrackerFactory.create(dataFormat, startResponse.getSegmentConsistentHash(), startResponse.getTopologyId(), segments);
   }
}
