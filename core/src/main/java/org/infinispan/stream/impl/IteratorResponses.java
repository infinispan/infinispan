package org.infinispan.stream.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Stream;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Response object used when an iterator is the response value and it is unknown if the iterator has enough entries
 * for the given batch size. The {@link RemoteResponse} is used by a remote node to return and this will be
 * externalized into a {@link BatchResponse} or {@link LastResponse} on the requesting node depending on if the iterator
 * has seen all entries.
 * @author wburns
 * @since 9.0
 */
public abstract class IteratorResponses implements IteratorResponse {
   private final static Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   private final Iterator<Object> iterator;

   IteratorResponses(Iterator<Object> iterator) {
      this.iterator = iterator;
   }

   @Override
   public Iterator<Object> getIterator() {
      return iterator;
   }

   /**
    * This response is used by remote nodes only - it is magically serialized into the below classes when deserialized
    * based on if the iterator is exhausted (see {@link IteratorResponsesExternalizer}
    */
   static class RemoteResponse extends IteratorResponses {
      private final Set<Integer> suspectedSegments;
      private final long batchSize;

      RemoteResponse(Iterator<Object> iterator, Set<Integer> suspectedSegments, long batchSize) {
         super(iterator);
         this.suspectedSegments = suspectedSegments;
         this.batchSize = batchSize;
      }

      @Override
      public Set<Integer> getSuspectedSegments() {
         return suspectedSegments;
      }
   }

   private static class BatchResponse extends IteratorResponses {

      BatchResponse(Iterator<Object> iterator) {
         super(iterator);
      }

      @Override
      public Set<Integer> getSuspectedSegments() {
         return Collections.emptySet();
      }

      @Override
      public String toString() {
         return "BatchResponse - more values required";
      }
   }

   private static class LastResponse extends IteratorResponses {
      private final Set<Integer> suspectedSegments;

      LastResponse(Iterator<Object> iterator, Set<Integer> suspectedSegments) {
         super(iterator);
         this.suspectedSegments = suspectedSegments;
      }

      @Override
      public boolean isComplete() {
         return true;
      }

      @Override
      public Set<Integer> getSuspectedSegments() {
         return suspectedSegments;
      }

      @Override
      public String toString() {
         return "LastResponse {suspectedSegments=" + suspectedSegments + "}";
      }
   }

   @Override
   public boolean isComplete() {
      return false;
   }

   /**
    * This externalizer is a special breed that converts a given response into others, based on whether or not
    * an iterator has completed or not. This allows the originator to not have to create an intermediate collection
    * to store the batched iterator and then the originator only has to create an object that requires given values.
    */
   public static class IteratorResponsesExternalizer extends AbstractExternalizer<IteratorResponses> {

      @Override
      public Integer getId() {
         return Ids.STREAM_ITERATOR_RESPONSE;
      }

      @Override
      public Set<Class<? extends IteratorResponses>> getTypeClasses() {
         // We only support
         return Collections.singleton(RemoteResponse.class);
      }

      @Override
      public void writeObject(ObjectOutput output, IteratorResponses object) throws IOException {
         // This special handling is because we don't know if we are done with the iterator until we have iterated
         // upon it
         RemoteResponse resp = (RemoteResponse) object;
         Iterator<Object> iter = resp.getIterator();
         long i = 0;
         for (; i < resp.batchSize && iter.hasNext(); ++i) {
            output.writeObject(iter.next());
         }
         output.writeObject(EndIterator.getInstance());
         // If there are no more entries that mean we are completed.
         boolean completed = !iter.hasNext();
         log.tracef("Sending %s entries to requestor and was complete? %s", String.valueOf(i), completed);
         output.writeBoolean(completed);
         if (completed) {
            // The final response will show all of the suspected segments if there are any
            Set<Integer> segments = resp.getSuspectedSegments();
            // This set is written to from other threads - so we have to synchronize to guarantee read visibility
            // The hasNext call above should have unregistered LocalStreamManagerImpl.SegmentListener
            synchronized (segments) {
               output.writeObject(segments);
            }
         }
      }

      @Override
      public IteratorResponses readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Object object = input.readObject();
         Iterator<Object> iterator;
         if (object != EndIterator.getInstance()) {
            Stream.Builder<Object> builder = Stream.builder();
            builder.accept(object);
            while ((object = input.readObject()) != EndIterator.getInstance()) {
               builder.accept(object);
            }
            iterator = builder.build().iterator();
         } else {
            iterator = Collections.emptyIterator();
         }
         boolean complete = input.readBoolean();
         if (complete) {
            return new LastResponse(iterator, (Set<Integer>) input.readObject());
         } else {
            return new BatchResponse(iterator);
         }
      }
   }
}
