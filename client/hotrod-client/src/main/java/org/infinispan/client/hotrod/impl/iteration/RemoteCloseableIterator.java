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
import org.infinispan.commons.marshall.Marshaller;
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
   private boolean endOfIteration = false;
   private boolean closed;
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
      if (!closed) {
         try {
            IterationEndResponse endResponse = operationsFactory.newIterationEndOperation(iterationId, transport).execute();
            short status = endResponse.getStatus();

            if (HotRodConstants.isSuccess(status)) {
               log.iterationClosed(iterationId);
            }
            if (HotRodConstants.isInvalidIteration(status)) {
               throw log.errorClosingIteration(iterationId);
            }
         } catch (TransportException te) {
            log.ignoringServerUnreachable(iterationId);
         } finally {
            closed = true;
         }
      }
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
         startInternal(segmentKeyTracker.missedSegments());
         fetch();
      }
   }

   private IterationStartResponse startInternal(Set<Integer> segments) {
      if (log.isDebugEnabled()) {
         log.debugf("Starting iteration with segments %s", segments);
      }
      IterationStartOperation iterationStartOperation = operationsFactory.newIterationStartOperation(filterConverterFactory, filterParams, segments, batchSize, metadata);
      IterationStartResponse startResponse = iterationStartOperation.execute();
      this.transport = startResponse.getTransport();
      if (log.isDebugEnabled()) {
         log.iterationTransportObtained(transport, iterationId);
      }
      this.iterationId = startResponse.getIterationId();
      if (log.isDebugEnabled()) {
         log.startedIteration(iterationId);
      }
      return startResponse;
   }

   public void start() {
      IterationStartResponse startResponse = startInternal(segments);
      Marshaller marshaller = startResponse.getTransport().getTransportFactory().getMarshaller();
      this.segmentKeyTracker = KeyTrackerFactory.create(
              marshaller, startResponse.getSegmentConsistentHash(), startResponse.getTopologyId(), segments);
   }
}
