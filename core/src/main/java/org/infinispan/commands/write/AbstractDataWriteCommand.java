/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.commands.write;

import org.infinispan.commands.read.AbstractDataCommand;
import org.infinispan.container.entries.ClusteredRepeatableReadEntry;
import org.infinispan.context.Flag;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Stuff common to WriteCommands
 *
 * @author Manik Surtani
 * @since 4.0
 */
public abstract class AbstractDataWriteCommand extends AbstractDataCommand implements DataWriteCommand {

   //Pedro -- indicates if the write skew is needed for this key
   private boolean writeSkewNeeded = false;

   protected AbstractDataWriteCommand() {
   }

   protected AbstractDataWriteCommand(Object key, Set<Flag> flags) {
      super(key, flags);
   }

   public Set<Object> getAffectedKeys() {
      return Collections.singleton(key);
   }

   @Override
   public boolean isReturnValueExpected() {
      return flags == null || !flags.contains(Flag.SKIP_REMOTE_LOOKUP);
   }

   //Pedro stuff

   /**
    * It serialize the key to send depending if the write skew must be performed on this key
    *
    * @return the key wrapped if write skew is needed, otherwise return the key unchanged
    */
   protected Object serializeKey() {
      if (writeSkewNeeded) {
         return new WriteSkewOnKey(key);
      } else {
         return key;
      }
   }

   /**
    * It de-serialize the key object received and mark it for write skew check if the key is wrapped
    *
    * @param object return the original key
    */
   protected void deserializeKey(Object object) {
      if (object instanceof WriteSkewOnKey) {
         writeSkewNeeded = true;
         this.key = ((WriteSkewOnKey)object).key;
      } else {
         writeSkewNeeded = false;
         this.key = object;
      }
   }

   /**
    * In local, it set the boolean to true if the write skew check must be performed in this key.
    * In remote, it marks the entry for write skew check if it is needed
    *
    * @param entry the entry corresponding to this key
    * @param local true if the command is executed locally
    */
   protected void checkIfWriteSkewNeeded(ClusteredRepeatableReadEntry entry, boolean local) {
      if(local) {
         //Pedro -- locally, check if the entry is marked for write skew check
         writeSkewNeeded = entry.isMarkedForWriteSkew();
      } else if(writeSkewNeeded) {
         //Pedro -- remotely, if the writeSkewCheck boolean is set to true, then mark the entry
         //for write skew check
         entry.markForWriteSkewCheck();
      }
   }

   /**
    * before sending the key, it is wrapped in this keys if the keys needs to perform the write skew check
    */
   private static class WriteSkewOnKey implements Serializable {
      private Object key;

      public WriteSkewOnKey(Object key) {
         this.key = key;
      }
   }

   /**
    * Helper for the PutMapCommand. it wrappers the keys needing write skew check in the class above.
    * @param original the original map
    * @param keysNeededWriteSkew the set with the keys needing write skew check
    * @return a map with the keys needing write skew check wrapper
    */
   public static Map<Object, Object> serializeKeys(Map<Object, Object> original, Set<Object> keysNeededWriteSkew) {
      Map<Object, Object> mapToSend = new HashMap<Object, Object>(original.size());

      if (keysNeededWriteSkew.isEmpty()) {
         mapToSend.putAll(original);
         return mapToSend;
      }

      for (Map.Entry<Object, Object> entry : original.entrySet()) {
         Object key = entry.getKey();

         if(keysNeededWriteSkew.contains(entry.getKey())) {
            mapToSend.put(new WriteSkewOnKey(key), entry.getValue());
         } else {
            mapToSend.put(key, entry.getValue());
         }
      }
      return mapToSend;
   }

   /**
    * Helper for the PutMapCommand. It does the reverse than the method above
    * @param received the map received
    * @param keysNeedWriteSkew the set to put the keys needed write skew
    * @return the original map
    */
   public static Map<Object, Object> deserializeKeys(Map<Object, Object> received, Set<Object> keysNeedWriteSkew) {
      Map<Object, Object> map = new HashMap<Object, Object>(received.size());

      for (Map.Entry<Object, Object> entry : received.entrySet()) {
         Object key = entry.getKey();

         if(key instanceof WriteSkewOnKey) {
            map.put(((WriteSkewOnKey) key).key, entry.getValue());
            keysNeedWriteSkew.add(((WriteSkewOnKey) key).key);
         } else {
            map.put(key, entry.getValue());
         }
      }

      return map;
   }

   /**
    * Helper for ClearCommand and PutMapCommand. In local mode, the keys that needs write skew check are added to the
    * the set.
    * In remote, the keys in the set are marked for rollback
    * @param entry the entry to be check
    * @param local true if it is executed locally
    * @param keysNeedWriteSkew the set of keys needed write skew
    */
   public static void checkIfWriteSkewNeeded(ClusteredRepeatableReadEntry entry, boolean local,
                                             Set<Object> keysNeedWriteSkew) {
      if(local) {
         //Pedro -- locally, check if the entry is marked for write skew check
         if(entry.isMarkedForWriteSkew()) {
            keysNeedWriteSkew.add(entry.getKey());
         }
      } else if(keysNeedWriteSkew.contains(entry.getKey())) {
         //Pedro -- remotely, if the key exists in the set, then mark the entry
         //for write skew check
         entry.markForWriteSkewCheck();
      }
   }
}
