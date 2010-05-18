package org.infinispan.client.hotrod.impl;

/**
 * Used for un/marshalling objects sent between hotrod client and server (hotrod is a binary protocol). 
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public interface HotRodMarshaller {

   byte[] marshallObject(Object toMarshall);

   Object readObject(byte[] bytes);
}
