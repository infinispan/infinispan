package org.infinispan.client.hotrod.impl;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public interface HotrodMarshaller {

   byte[] marshallObject(Object toMarshall);

   Object readObject(byte[] bytes);
}
