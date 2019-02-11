package org.infinispan.commons.hash;

/**
 * Interface that governs implementations
 *
 * @author Manik Surtani
 * @author Patrick McFarland
 * @see MurmurHash3
 */

public interface Hash {
   /**
    * Hashes a byte array efficiently.
    *
    * @param payload a byte array to hash
    * @return a hash code for the byte array
    */
   int hash(byte[] payload);

   /**
    * An incremental version of the hash function, that spreads a pre-calculated
    * hash code, such as one derived from {@link Object#hashCode()}.
    *
    * @param hashcode an object's hashcode
    * @return a spread and hashed version of the hashcode
    */
   int hash(int hashcode);

   /**
    * A helper that calculates the hashcode of an object, choosing the optimal
    * mechanism of hash calculation after considering the type of the object
    * (byte array, String or Object).
    *
    * @param o object to hash
    * @return a hashcode
    */
   int hash(Object o);
}
