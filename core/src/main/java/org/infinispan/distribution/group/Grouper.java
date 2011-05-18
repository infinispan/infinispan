package org.infinispan.distribution.group;

/**
 * User applications may implement this interface in order to customize the compution of groups in cases when the modifying the
 * key is not possible.
 * 
 * This acts as an interceptor, passing the previously computed value in. Initially the passed in group will be that extracted
 * from the @Group annotation
 * 
 * @see Group
 * 
 * @author Pete Muir
 * 
 * @param <T>
 */
public interface Grouper<T> {

    /**
     * Compute the group for a given key
     * 
     * @param key the key to compute the group for
     * @param group the group as currently computed, or null if no group has been determined yet
     * @return the group, or null if no group is defined
     */
    String computeGroup(T key, String group);
    
    Class<T> getKeyType();

}