package org.infinispan.persistence.keymappers;

/**
 * Extends {@link Key2StringMapper} and allows a bidirectional transformation between keys and Strings.  Note that the
 * object instance created by {@link #getKeyMapping(String)} is guaranteed to be <i>equal</i> to the original object
 * used to generate the String, but not necessarily the same object reference.
 * <p />
 * The following condition should be satisfied by implementations of this interface:
 * <code>
 *   assert key.equals(mapper.getKeyMapping(mapper.getStringMapping(key)));
 * </code>
 *
 * @author Mircea.Markus@jboss.com
 * @author Manik Surtani
 * @since 4.1
 */
public interface TwoWayKey2StringMapper extends Key2StringMapper {
   /**
    * Maps a String back to its original key
    * @param stringKey string representation of a key
    * @return an object instance that is <i>equal</i> to the original object used to create the key mapping.
    */
   Object getKeyMapping(String stringKey);
}
