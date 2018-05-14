package org.infinispan.remoting.transport;

/**
 * A destination for an Infinispan command or operation.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface Address extends Comparable<Address> {
   Address[] EMPTY_ARRAY = new Address[0];
}
