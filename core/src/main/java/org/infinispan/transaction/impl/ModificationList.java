package org.infinispan.transaction.impl;

import java.util.AbstractList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.Flag;
import org.infinispan.context.impl.FlagBitSets;

import net.jcip.annotations.GuardedBy;

/**
 * A list of {@link WriteCommand} for a transaction
 * <p>
 * {@link WriteCommand} can only be appended and the methods {@link #getAllModifications()} and {@link
 * #getModifications()} return a snapshot of the current list. {@link WriteCommand} appended after those methods are
 * invoked, are not visible.
 *
 * @since 14.0
 */
public final class ModificationList {

   private static final int DEFAULT_ARRAY_LENGTH = 16;

   @GuardedBy("this")
   private WriteCommand[] mods;
   @GuardedBy("this")
   private int nextInsertIndex;
   @GuardedBy("this")
   private int[] nonLocalIndexes;
   @GuardedBy("this")
   private int nextNonLocalInsertIndex;
   private volatile boolean frozen;

   public ModificationList() {
      mods = new WriteCommand[DEFAULT_ARRAY_LENGTH];
      nonLocalIndexes = new int[DEFAULT_ARRAY_LENGTH];
   }

   public ModificationList(int capacity) {
      if (capacity < 1) {
         throw new IllegalArgumentException("Capacity must be greater than 1 but is " + capacity);
      }
      mods = new WriteCommand[capacity];
      nonLocalIndexes = new int[capacity];
   }

   public static ModificationList fromCollection(Collection<WriteCommand> mods) {
      if (mods == null || mods.size() == 0) {
         return new ModificationList();
      }
      ModificationList modificationList = new ModificationList(mods.size());
      for (WriteCommand command : mods) {
         modificationList.append(command);
      }
      return modificationList;
   }

   /**
    * Appends the {@link WriteCommand} to this list.
    *
    * @param command The {@link WriteCommand} instance to append.
    * @throws IllegalStateException If this list is frozen.
    * @see #freeze()
    */
   public synchronized void append(WriteCommand command) {
      checkNotFrozen();
      if (nextInsertIndex == mods.length) {
         growAllMods();
      }
      assert nextInsertIndex < mods.length;
      if (!command.hasAnyFlag(FlagBitSets.CACHE_MODE_LOCAL)) {
         if (nextNonLocalInsertIndex == nonLocalIndexes.length) {
            growNonLocalIndexMap();
         }
         assert nextNonLocalInsertIndex < nonLocalIndexes.length;
         nonLocalIndexes[nextNonLocalInsertIndex++] = nextInsertIndex;
      }
      mods[nextInsertIndex++] = command;
   }

   /**
    * Freezes this list.
    * <p>
    * After invoked, no more {@link WriteCommand} can be appended to this list. {@link #append(WriteCommand)} will throw
    * a {@link IllegalStateException}.
    */
   public void freeze() {
      frozen = true;
   }

   /**
    * Returns a snapshot of this list.
    * <p>
    * This snapshot does not contain {@link WriteCommand} with flag {@link Flag#CACHE_MODE_LOCAL} and it cannot be
    * modified.
    *
    * @return A snapshot of this list.
    */
   public synchronized List<WriteCommand> getModifications() {
      return new NonLocalModificationsList(mods, nonLocalIndexes, nextNonLocalInsertIndex);
   }

   /**
    * @return {@code true} if it contains one or more {@link WriteCommand} without {@link Flag#CACHE_MODE_LOCAL}.
    */
   public synchronized boolean hasNonLocalModifications() {
      return nextNonLocalInsertIndex != 0;
   }

   /**
    * @return A snapshot of this list with all {@link WriteCommand}. The {@link List} cannot be modified.
    * @see #getModifications()
    */
   public synchronized List<WriteCommand> getAllModifications() {
      return new AllModsList(mods, nextInsertIndex);
   }

   /**
    * @return The number of {@link WriteCommand} stored this list.
    */
   public synchronized int size() {
      return nextInsertIndex;
   }

   /**
    * @return {@code true} if this list is empty.
    */
   public synchronized boolean isEmpty() {
      return nextInsertIndex == 0;
   }

   @Override
   public synchronized String toString() {
      return "ModificationList{" +
            "mods=" + Arrays.toString(mods) +
            '}';
   }

   private void checkNotFrozen() {
      if (frozen) {
         throw new IllegalStateException();
      }
   }

   @GuardedBy("this")
   private void growAllMods() {
      int newCapacity = mods.length == 1 ? 2 : mods.length + (mods.length >> 1);
      mods = Arrays.copyOf(mods, newCapacity);
   }

   @GuardedBy("this")
   private void growNonLocalIndexMap() {
      int newCapacity = nonLocalIndexes.length == 1 ? 2 : nonLocalIndexes.length + (nonLocalIndexes.length >> 1);
      nonLocalIndexes = Arrays.copyOf(nonLocalIndexes, newCapacity);
   }

   private static void checkIndex(int index, int size) {
      if (index < 0 || index >= size) {
         throw new ArrayIndexOutOfBoundsException("Range check failed: " + index + " not in [0," + size + "[.");
      }
   }

   private static class AllModsList extends AbstractList<WriteCommand> {

      private final WriteCommand[] mods;
      private final int size;

      private AllModsList(WriteCommand[] mods, int size) {
         this.mods = mods;
         this.size = size;
      }

      @Override
      public WriteCommand get(int index) {
         checkIndex(index, size);
         return mods[index];
      }

      @Override
      public int size() {
         return size;
      }
   }

   private static class NonLocalModificationsList extends AbstractList<WriteCommand> {

      private final WriteCommand[] mods;
      private final int[] indexMappings;
      private final int size;

      private NonLocalModificationsList(WriteCommand[] mods, int[] indexMappings, int size) {
         this.mods = mods;
         this.indexMappings = indexMappings;
         this.size = size;
      }

      @Override
      public WriteCommand get(int index) {
         checkIndex(index, size);
         return mods[indexMappings[index]];
      }

      @Override
      public int size() {
         return size;
      }
   }

}
