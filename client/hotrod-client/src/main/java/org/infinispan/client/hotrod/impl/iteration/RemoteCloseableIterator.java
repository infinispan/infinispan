package org.infinispan.client.hotrod.impl.iteration;

import net.jcip.annotations.NotThreadSafe;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.impl.operations.IterationEndResponse;
import org.infinispan.client.hotrod.impl.operations.IterationNextOperation;
import org.infinispan.client.hotrod.impl.operations.IterationNextResponse;
import org.infinispan.client.hotrod.impl.operations.IterationStartOperation;
import org.infinispan.client.hotrod.impl.operations.IterationStartResponse;
import org.infinispan.client.hotrod.impl.operations.OperationsFactory;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.CloseableIterator;

import java.util.AbstractMap.SimpleEntry;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;

import static org.infinispan.client.hotrod.impl.protocol.HotRodConstants.INVALID_ITERATION;
import static org.infinispan.client.hotrod.impl.protocol.HotRodConstants.NO_ERROR_STATUS;
import static org.infinispan.client.hotrod.marshall.MarshallerUtil.bytes2obj;

/**
 * @author gustavonalle
 * @since 8.0
 */
@NotThreadSafe
public class RemoteCloseableIterator implements CloseableIterator<Entry<Object, Object>> {

   private static final Log log = LogFactory.getLog(RemoteCloseableIterator.class);

   private final OperationsFactory operationsFactory;
   private final Marshaller marshaller;
   private final String filterConverterFactory;
   private final Set<Integer> segments;
   private final int batchSize;

   private KeyTracker segmentKeyTracker;
   private Transport transport;
   private String iterationId;
   boolean endOfIteration = false;
   private Queue<SimpleEntry<Object, Object>> nextElements = new LinkedList<>();

   public RemoteCloseableIterator(OperationsFactory operationsFactory, String filterConverterFactory, Set<Integer> segments, int batchSize, Marshaller marshaller) {
      this.filterConverterFactory = filterConverterFactory;
      this.segments = segments;
      this.batchSize = batchSize;
      this.operationsFactory = operationsFactory;
      this.marshaller = marshaller;
   }

   @Override
   public void close() {
      IterationEndResponse endResponse = operationsFactory.newIterationEndOperation(iterationId, transport).execute();
      short status = endResponse.getStatus();

      if (status == NO_ERROR_STATUS) {
         log.iterationClosed(iterationId);
      }
      if (endResponse.getStatus() == INVALID_ITERATION) {
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
   public Entry<Object, Object> next() {
      if (!hasNext()) throw new NoSuchElementException();
      return nextElements.remove();
   }

   private void fetch() {
      try {
         IterationNextOperation iterationNextOperation = operationsFactory.newIterationNextOperation(iterationId, transport);

         while (nextElements.isEmpty() && !endOfIteration) {
            IterationNextResponse iterationNextResponse = iterationNextOperation.execute();
            short status = iterationNextResponse.getStatus();
            if (status == INVALID_ITERATION) {
               throw log.errorRetrievingNext(iterationId);
            }
            Entry<byte[], byte[]>[] entries = iterationNextResponse.getEntries();

            if (entries.length == 0) {
               endOfIteration = true;
               break;
            }
            for (Entry<byte[], byte[]> entry : entries) {
               if (segmentKeyTracker.track(entry.getKey())) {
                  nextElements.add(new SimpleEntry<>(unmarshall(entry.getKey()), unmarshall(entry.getValue())));
               }
            }
            segmentKeyTracker.segmentsFinished(iterationNextResponse.getFinishedSegments());
         }

      } catch (TransportException e) {
         log.warnf(e, "Error reaching the server during iteration");
         restartIteration(segmentKeyTracker.missedSegments());
         fetch();
      }
   }


   private Object unmarshall(byte[] bytes) {
      return bytes2obj(marshaller, bytes);
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
      IterationStartOperation iterationStartOperation = operationsFactory.newIterationStartOperation(filterConverterFactory, fromSegments, batchSize);
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
