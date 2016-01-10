package org.infinispan.client.hotrod.impl.iteration;

import net.jcip.annotations.NotThreadSafe;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.impl.operations.IterationEndResponse;
import org.infinispan.client.hotrod.impl.operations.IterationNextOperation;
import org.infinispan.client.hotrod.impl.operations.IterationNextResponse;
import org.infinispan.client.hotrod.impl.operations.IterationStartOperation;
import org.infinispan.client.hotrod.impl.operations.IterationStartResponse;
import org.infinispan.client.hotrod.impl.operations.OperationsFactory;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.util.CloseableIterator;

import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;

/**
 * @author gustavonalle
 * @since 8.0
 */
@NotThreadSafe
public class RemoteCloseableIterator<E> implements CloseableIterator<Entry<Object, E>> {

   private static final Log log = LogFactory.getLog(RemoteCloseableIterator.class);

   private final OperationsFactory operationsFactory;
   private final String filterConverterFactory;
   private final byte[][] filterParams;
   private final Set<Integer> segments;
   private final int batchSize;
   private final boolean metadata;

   private KeyTracker segmentKeyTracker;
   private Transport transport;
   private String iterationId;
   boolean endOfIteration = false;
   private Queue<Entry<Object, E>> nextElements = new LinkedList<>();

   public RemoteCloseableIterator(OperationsFactory operationsFactory, String filterConverterFactory,
                                  byte[][] filterParams, Set<Integer> segments, int batchSize, boolean metadata) {
      this.filterConverterFactory = filterConverterFactory;
      this.filterParams = filterParams;
      this.segments = segments;
      this.batchSize = batchSize;
      this.operationsFactory = operationsFactory;
      this.metadata = metadata;
   }

   public RemoteCloseableIterator(OperationsFactory operationsFactory, int batchSize, Set<Integer> segments, boolean metadata) {
      this(operationsFactory, null, null, segments, batchSize, metadata);
   }

   @Override
   public void close() {
      IterationEndResponse endResponse = operationsFactory.newIterationEndOperation(iterationId, transport).execute();
      short status = endResponse.getStatus();

      if (HotRodConstants.isSuccess(status)) {
         log.iterationClosed(iterationId);
      }
      if (HotRodConstants.isInvalidIteration(status)) {
         throw log.errorClosingIteration(iterationId);
      }
   }

   @Override
   public boolean hasNext() {
      if (nextElements.isEmpty()) {
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
      try {
         IterationNextOperation<E> iterationNextOperation = operationsFactory.newIterationNextOperation(iterationId, transport, segmentKeyTracker);

         while (nextElements.isEmpty() && !endOfIteration) {
            IterationNextResponse<E> iterationNextResponse = iterationNextOperation.execute();
            if (!iterationNextResponse.hasMore()) {
               endOfIteration = true;
               break;
            }
            nextElements.addAll(iterationNextResponse.getEntries());
         }

      } catch (TransportException e) {
         log.warnf(e, "Error reaching the server during iteration");
         restartIteration(segmentKeyTracker.missedSegments());
         fetch();
      }
   }

   private void restartIteration(Set<Integer> missedSegments) {
      startInternal(missedSegments);
   }

   private void start(Set<Integer> fromSegments) {
      IterationStartResponse startResponse = startInternal(fromSegments);

      this.segmentKeyTracker = KeyTrackerFactory.create(startResponse.getSegmentConsistentHash(), startResponse.getTopologyId());
   }

   private IterationStartResponse startInternal(Set<Integer> fromSegments) {
      if (log.isDebugEnabled()) {
         log.debugf("Staring iteration with segments %s", fromSegments);
      }
      IterationStartOperation iterationStartOperation = operationsFactory.newIterationStartOperation(filterConverterFactory, filterParams, fromSegments, batchSize, metadata);
      IterationStartResponse startResponse = iterationStartOperation.execute();
      this.transport = startResponse.getTransport();
      if (log.isDebugEnabled()) {
         log.debugf("Obtained transport", this.transport);
      }
      this.iterationId = startResponse.getIterationId();
      if (log.isDebugEnabled()) {
         log.debugf("IterationId:", this.iterationId);
      }
      return startResponse;
   }

   public void start() {
      start(segments);
   }
}
