package org.infinispan.commons.marshall.jboss;

/**
 * RiverCloseListener is used by Infinispan's extension of River Marshaller and Unmarshaller
 * so that pools can be notified of instances not being in use anymore.
 *
 * @author Sanne Grinovero
 * @since 5.1
 */
public interface RiverCloseListener {

   void closeMarshaller();

   void closeUnmarshaller();

}
