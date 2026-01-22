package org.infinispan.commons.spi;

/**
 * Interface for vector optimized operations.
 */
public interface Vectors {
   /**
    * Checks a susbet of bytes within two different arrays to test if they equal or not. Note that offsets can vary
    * between both arrays, however the total length (to - from) must be equal.
    * @param a The first array to test equality of
    * @param aFromIndex the offset into the array to test bytes of
    * @param aToIndex the end offset of the array to test bytes of
    * @param b The second array to test equality of
    * @param bFromIndex the offset into the array to test bytes of
    * @param bToIndex the end offset of the array to test bytes of
    * @return whether the bytes within the two arrays are equal
    */
   boolean arraysEqual(byte[] a, int aFromIndex, int aToIndex, byte[] b, int bFromIndex, int bToIndex);

   /**
    * Calculates the minimum distance between a segment hash and a set of node hashes.
    * The distance metric is specific to the consistent hash implementation (ring distance).
    *
    * @param segmentHash the hash of the segment
    * @param nodeHashes the hashes of the node (sorted)
    * @return the minimum distance
    */
   long minDistance(long segmentHash, long[] nodeHashes);
}
