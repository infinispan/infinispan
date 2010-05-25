package org.infinispan.client.hotrod;

import net.jcip.annotations.ThreadSafe;

/**
 * Used for un/marshalling objects sent between hotrod client and server (hotrod is a binary protocol).
 * A single instance of this class is shared by all threads, so this class needs to be thread safe.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@ThreadSafe 
public interface HotRodMarshaller {

   byte[] marshallObject(Object toMarshall);

   Object readObject(byte[] bytes);
}
