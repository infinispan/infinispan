package org.infinispan.distribution.group;

import org.infinispan.remoting.transport.Address;

/**
 * Control's key grouping.
 *
 * @author Pete Muir
 */
public interface GroupManager {

	/**
	 * Get the group for a given key
	 *
	 * @param key the key for which to get the group
	 * @return the group, or null if no group is defined for the key
	 */
	String getGroup(Object key);

   /**
    * Checks if this node is an owner of the group.
    *
    * @param group the group name.
    * @return {@code true} if this node is an owner of the group, {@code false} otherwise.
    */
   boolean isOwner(String group);

   /**
    * It returns the primary owner of the group.
    *
    * @param group the group name.
    * @return the primary owner of the group.
    */
   Address getPrimaryOwner(String group);

   /**
    * It checks if this node is the primary owner of the group.
    *
    * @param group the group name.
    * @return {@code true} if this node is the primary owner of the group, {@code false} otherwise.
    */
   boolean isPrimaryOwner(String group);

}
