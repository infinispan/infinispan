/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.nearcache.jms;

import org.infinispan.Cache;
import org.infinispan.CacheException;
import org.infinispan.marshall.StreamingMarshaller;
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
