package org.infinispan.client.hotrod;

import net.jcip.annotations.ThreadSafe;

import java.util.Properties;

/**
 * Used for un/marshalling objects sent between hotrod client and server (hotrod is a binary protocol).
 * A single instance of this class is shared by all threads, so this class needs to be thread safe.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@ThreadSafe 
public interface HotRodMarshaller {

   void init(Properties config);

   byte[] marshallObject(Object toMarshall);

   Object readObject(byte[] bytes);
}
