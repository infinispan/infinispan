package org.infinispan.commons.hash;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.UUID;

import org.infinispan.commons.marshall.WrappedBytes;
import org.junit.jupiter.api.Test;

public class MurmurHash3ObjectHashTest {

   private static final MurmurHash3 HASH = MurmurHash3.getInstance();
   private static final int SEED = 0;

   @Test
   public void testLongsWithSameHashCodeHashDifferently() {
      Long a = 0L;
      Long b = 0x1_0000_0001L;
      assertThat(a.hashCode()).isEqualTo(b.hashCode());

      assertThat(HASH.MurmurHash3_x64_64(a, SEED)).isNotEqualTo(HASH.MurmurHash3_x64_64(b, SEED));
   }

   @Test
   public void testUuidsWithSameHashCodeHashDifferently() {
      UUID a = new UUID(0, 0);
      UUID b = new UUID(1, 1);
      assertThat(a.hashCode()).isEqualTo(b.hashCode());

      assertThat(HASH.MurmurHash3_x64_64(a, SEED)).isNotEqualTo(HASH.MurmurHash3_x64_64(b, SEED));
   }

   @Test
   public void testEmptyLongArrayHashesConsistently() {
      // Hash through the Object overload, the one the hot key tracker uses.
      Object first = new long[0];
      Object second = new long[0];

      assertThat(HASH.MurmurHash3_x64_64(first, SEED)).isEqualTo(HASH.MurmurHash3_x64_64(second, SEED));
   }

   @Test
   public void testWrappedBytesHashesOnlyItsView() {
      byte[] content = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17};

      // Same content at different offsets, surrounded by different bytes.
      WrappedBytes first = new OffsetBytes(new byte[] {-1, -1, -1}, content, new byte[] {-1});
      WrappedBytes second = new OffsetBytes(new byte[] {42}, content, new byte[] {7, 7, 7, 7});

      long expected = MurmurHash3.MurmurHash3_x64_64(content, SEED);
      assertThat(HASH.MurmurHash3_x64_64(first, SEED)).isEqualTo(expected);
      assertThat(HASH.MurmurHash3_x64_64(second, SEED)).isEqualTo(expected);
   }

   @Test
   public void testEmptyRangeOutsideArrayIsRejected() {
      byte[] key = new byte[10];

      // Nothing is read for an empty range, so only an explicit bounds check rejects it.
      assertThatThrownBy(() -> MurmurHash3.MurmurHash3_x64_64(key, 100, 0, SEED))
            .isInstanceOf(IndexOutOfBoundsException.class);
   }

   @Test
   public void testWrappedBytesWithoutBackingArrayHashesByContent() {
      Object first = new NoArrayBytes(new byte[] {1, 2, 3, 4});
      Object second = new NoArrayBytes(new byte[] {1, 2, 3, 4});

      assertThat(HASH.MurmurHash3_x64_64(first, SEED)).isEqualTo(HASH.MurmurHash3_x64_64(second, SEED));
   }

   /**
    * A {@link WrappedBytes} that exposes no backing array, as the interface allows.
    */
   private static final class NoArrayBytes implements WrappedBytes {
      private final byte[] content;

      /**
       * Creates a view over a private copy of {@code content}.
       *
       * @param content the bytes this instance exposes
       */
      NoArrayBytes(byte[] content) {
         this.content = content.clone();
      }

      /**
       * {@inheritDoc}
       *
       * @return always {@code null}, this instance has no backing array
       */
      @Override
      public byte[] getBytes() {
         return null;
      }

      /**
       * {@inheritDoc}
       *
       * @return always {@code -1}, this instance has no backing array
       */
      @Override
      public int backArrayOffset() {
         return -1;
      }

      /**
       * {@inheritDoc}
       *
       * @return the number of bytes in this view
       */
      @Override
      public int getLength() {
         return content.length;
      }

      /**
       * {@inheritDoc}
       *
       * @param offset the position inside this view
       * @return the byte at that position
       */
      @Override
      public byte getByte(int offset) {
         return content[offset];
      }

      /**
       * Hashes the bytes of this view.
       *
       * @return a hash code consistent with {@link #equals(Object)}
       */
      @Override
      public int hashCode() {
         return Arrays.hashCode(content);
      }

      /**
       * Compares the bytes of this view with another {@link WrappedBytes}.
       *
       * @param obj the object to compare with
       * @return {@code true} if {@code obj} is a {@link WrappedBytes} with the same content
       */
      @Override
      public boolean equals(Object obj) {
         return obj instanceof WrappedBytes wb && equalsWrappedBytes(wb);
      }
   }

   /**
    * A {@link WrappedBytes} whose content starts in the middle of its backing array.
    */
   private static final class OffsetBytes implements WrappedBytes {
      private final byte[] backing;
      private final int offset;
      private final int length;

      /**
       * Creates a view of {@code content} placed between {@code before} and {@code after} in one backing array.
       *
       * @param before  bytes placed ahead of the content
       * @param content the bytes this instance exposes
       * @param after   bytes placed after the content
       */
      OffsetBytes(byte[] before, byte[] content, byte[] after) {
         this.backing = new byte[before.length + content.length + after.length];
         System.arraycopy(before, 0, backing, 0, before.length);
         System.arraycopy(content, 0, backing, before.length, content.length);
         System.arraycopy(after, 0, backing, before.length + content.length, after.length);
         this.offset = before.length;
         this.length = content.length;
      }

      /**
       * {@inheritDoc}
       *
       * @return the whole backing array, including the bytes outside this view
       */
      @Override
      public byte[] getBytes() {
         return backing;
      }

      /**
       * {@inheritDoc}
       *
       * @return the index of the first byte of this view in the backing array
       */
      @Override
      public int backArrayOffset() {
         return offset;
      }

      /**
       * {@inheritDoc}
       *
       * @return the number of bytes in this view
       */
      @Override
      public int getLength() {
         return length;
      }

      /**
       * {@inheritDoc}
       *
       * @param offset the position inside this view
       * @return the byte at that position
       */
      @Override
      public byte getByte(int offset) {
         return backing[this.offset + offset];
      }

      /**
       * Hashes the bytes of this view only.
       *
       * @return a hash code consistent with {@link #equals(Object)}
       */
      @Override
      public int hashCode() {
         return Arrays.hashCode(Arrays.copyOfRange(backing, offset, offset + length));
      }

      /**
       * Compares the bytes of this view with another {@link WrappedBytes}.
       *
       * @param obj the object to compare with
       * @return {@code true} if {@code obj} is a {@link WrappedBytes} with the same content
       */
      @Override
      public boolean equals(Object obj) {
         return obj instanceof WrappedBytes wb && equalsWrappedBytes(wb);
      }
   }
}
