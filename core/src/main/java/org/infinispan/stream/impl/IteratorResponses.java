package org.infinispan.stream.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.commons.marshall.UserObjectOutput;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
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
   private final Spliterator<Object> spliterator;

   IteratorResponses(Iterator<Object> iterator, Spliterator<Object> spliterator) {
      this.iterator = iterator;
      this.spliterator = spliterator;
   }

   public Iterator<Object> getIterator() {
      return iterator;
   }

   @Override
   public Spliterator<Object> getSpliterator() {
      return spliterator;
   }

   /**
    * This response is used by remote nodes only - it is magically serialized into the below classes when deserialized
    * based on if the iterator is exhausted (see {@link IteratorResponsesExternalizer}
    */
   static class RemoteResponse extends IteratorResponses {
      private final IntSet suspectedSegments;
      private final long batchSize;

      RemoteResponse(Iterator<Object> iterator, IntSet suspectedSegments, long batchSize) {
         super(iterator, null);
         this.suspectedSegments = suspectedSegments;
         this.batchSize = batchSize;
      }

      @Override
      public IntSet getSuspectedSegments() {
         return suspectedSegments;
      }
   }

   private static class BatchResponse extends IteratorResponses {

      BatchResponse(Spliterator<Object> spliterator) {
         super(null, spliterator);
      }

      @Override
      public IntSet getSuspectedSegments() {
         return IntSets.immutableEmptySet();
      }

      @Override
      public String toString() {
         return "BatchResponse - more values required";
      }
   }

   private static class LastResponse extends IteratorResponses {
      private final IntSet suspectedSegments;

      LastResponse(Spliterator<Object> spliterator, IntSet suspectedSegments) {
         super(null, spliterator);
         this.suspectedSegments = suspectedSegments;
      }

      @Override
      public boolean isComplete() {
         return true;
      }

      @Override
      public IntSet getSuspectedSegments() {
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
      public void writeObject(UserObjectOutput output, IteratorResponses object) throws IOException {
         // This special handling is because we don't know if we are done with the iterator until we have iterated
         // upon it
         RemoteResponse resp = (RemoteResponse) object;
         if (resp.batchSize > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("We don't yet support greater than int entries returned");
         }
         // This way we can allocate to fit in the batch size
         output.writeInt((int) resp.batchSize);
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
            // The hasNext call above should have unregistered LocalStreamManagerImpl.SegmentListener
            output.writeObject(resp.getSuspectedSegments());
         }
      }

      @Override
      public IteratorResponses readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         int batchSize = input.readInt();
         Object object = input.readObject();
         Spliterator<Object> spliterator;
         if (object != EndIterator.getInstance()) {
            Object[] results = new Object[batchSize];
            results[0] = object;
            int offset = 1;
            while ((object = input.readObject()) != EndIterator.getInstance()) {
               results[offset++] = object;
            }
            spliterator = Spliterators.spliterator(results, 0, offset, Spliterator.DISTINCT | Spliterator.NONNULL);
         } else {
            spliterator = Spliterators.emptySpliterator();
         }
         boolean complete = input.readBoolean();
         if (complete) {
            return new LastResponse(spliterator, (IntSet) input.readObject());
         } else {
            return new BatchResponse(spliterator);
         }
      }
   }
}
