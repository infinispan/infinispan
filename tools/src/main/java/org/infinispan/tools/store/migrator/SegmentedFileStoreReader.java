package org.infinispan.tools.store.migrator;

import static org.infinispan.tools.store.migrator.Element.LOCATION;
import static org.infinispan.tools.store.migrator.Element.SEGMENT_COUNT;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.infinispan.persistence.spi.MarshallableEntry;

public class SegmentedFileStoreReader implements StoreIterator {

   final int numSegments;
   final Function<StoreProperties, StoreIterator> storeFactory;

   final List<StoreIterator> storeIterators;

   public SegmentedFileStoreReader(StoreProperties properties, Function<StoreProperties, StoreIterator> storeFactory) {
      properties.required(SEGMENT_COUNT);

      this.numSegments = Integer.parseInt(properties.get(SEGMENT_COUNT));
      this.storeFactory = storeFactory;

      Path root = Path.of(properties.get(LOCATION));
      this.storeIterators = new ArrayList<>(numSegments);
      for (int i = 0; i < numSegments; i++) {
         StoreProperties p = new StoreProperties(properties);
         String segment = root.resolve(Integer.toString(i)).toString();
         p.put(segment, LOCATION);
         StoreIterator it = storeFactory.apply(p);
         storeIterators.add(it);
      }
   }

   @Override
   public void close() throws Exception {
      for (StoreIterator it : storeIterators)
         it.close();
   }

   @Override
   public Iterator<MarshallableEntry> iterator() {
      return new SegmentedStoreIterator();
   }

   class SegmentedStoreIterator implements Iterator<MarshallableEntry> {

      int currentSegment;
      List<Iterator<MarshallableEntry>> entryIterators = storeIterators.stream()
            .map(Iterable::iterator)
            .collect(Collectors.toList());

      @Override
      public boolean hasNext() {
         int nextSegment = currentSegment;
         while (nextSegment < numSegments) {
            if (entryIterators.get(nextSegment).hasNext())
               return true;
            nextSegment++;
         }
         return false;
      }

      @Override
      public MarshallableEntry next() {
         while (currentSegment < numSegments) {
            Iterator<MarshallableEntry> it = entryIterators.get(currentSegment);
            if (it.hasNext()) {
               return it.next();
            }
            currentSegment++;
         }
         throw new NoSuchElementException();
      }
   }
}
