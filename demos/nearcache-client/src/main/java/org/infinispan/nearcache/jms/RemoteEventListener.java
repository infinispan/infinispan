package org.infinispan.nearcache.jms;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.jms.BytesMessage;
import javax.jms.Message;
import javax.jms.MessageListener;

/**
 * JMS listener that listens for remote cache events and invalidates cache
 * contents.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public class RemoteEventListener implements MessageListener {

   private static final Log log = LogFactory.getLog(RemoteEventListener.class);

   final Cache<Object, Object> cache;
   final StreamingMarshaller marshaller;

   public RemoteEventListener(Cache<Object, Object> cache,
                              StreamingMarshaller marshaller) {
      this.cache = cache;
      this.marshaller = marshaller;
   }

   @Override
   public void onMessage(Message msg) {
      try {
         // Remove the key from the cache
         Object key = getKey(msg);
         log.infof("Received invalidation message[%s] for key=%s, remove from cache",
                   msg.getJMSMessageID(), key);
         cache.remove(key);
      } catch (Exception e) {
         throw new CacheException("Unable to process remote cache event", e);
      }
   }

   private Object getKey(Message message) throws Exception {
      BytesMessage msg = (BytesMessage) message;
      // Transform a Hot Rod binary key into the local Java equivalent
      byte[] keyBytes = new byte[(int) msg.getBodyLength()];
      msg.readBytes(keyBytes);
      // Since Hot Rod stores keys in byte[], it needs to be unmarshalled
      return marshaller.objectFromByteBuffer(keyBytes);
   }

}
