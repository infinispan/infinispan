package org.infinispan.jboss.marshalling.commons;

/**
 * RiverCloseListener is used by Infinispan's extension of River Marshaller and Unmarshaller
 * so that pools can be notified of instances not being in use anymore.
 *
 * @author Sanne Grinovero
 * @since 5.1
 * @deprecated since 11.0. To be removed in 14.0 ISPN-11947.
 */
@Deprecated
public interface RiverCloseListener {

   void closeMarshaller();

   void closeUnmarshaller();

}
