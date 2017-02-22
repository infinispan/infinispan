package org.infinispan.distribution;

/**
 * //TODO document this!
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public interface CommandPosition {

   boolean isNext();

   void finish();

}
