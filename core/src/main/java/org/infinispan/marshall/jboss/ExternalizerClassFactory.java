/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
package org.infinispan.marshall.jboss;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.WeakHashMap;

import net.jcip.annotations.Immutable;

import org.infinispan.CacheException;
import org.infinispan.remoting.RpcManager;
import org.infinispan.util.Util;
import org.jboss.marshalling.ClassExternalizerFactory;
import org.jboss.marshalling.Externalizer;

/**
 * CustomExternalizerFactory.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
@Immutable
public class ExternalizerClassFactory implements ClassExternalizerFactory {
   private static final Map<String, String> EXTERNALIZERS = new HashMap<String, String>();
   
   static {
      EXTERNALIZERS.put("org.infinispan.transaction.GlobalTransaction", "org.infinispan.marshall.jboss.GlobalTransactionExternalizer"); 
      EXTERNALIZERS.put("org.infinispan.remoting.transport.jgroups.JGroupsAddress", "org.infinispan.marshall.jboss.JGroupsAddressExternalizer"); 
      EXTERNALIZERS.put("java.util.ArrayList", "org.infinispan.marshall.jboss.ArrayListExternalizer");
      EXTERNALIZERS.put("java.util.LinkedList", "org.infinispan.marshall.jboss.LinkedListExternalizer");
      EXTERNALIZERS.put("java.util.HashMap", "org.infinispan.marshall.jboss.MapExternalizer");
      EXTERNALIZERS.put("java.util.TreeMap", "org.infinispan.marshall.jboss.MapExternalizer");
      EXTERNALIZERS.put("java.util.HashSet", "org.infinispan.marshall.jboss.SetExternalizer");
      EXTERNALIZERS.put("java.util.TreeSet", "org.infinispan.marshall.jboss.SetExternalizer");
      EXTERNALIZERS.put("org.infinispan.util.Immutables$ImmutableMapWrapper", "org.infinispan.marshall.jboss.ImmutableMapExternalizer");
      EXTERNALIZERS.put("org.infinispan.marshall.MarshalledValue", "org.infinispan.marshall.jboss.MarshalledValueExternalizer");
      EXTERNALIZERS.put("org.infinispan.util.FastCopyHashMap", "org.infinispan.marshall.jboss.MapExternalizer");
      EXTERNALIZERS.put("java.util.Collections$SingletonList", "org.infinispan.marshall.jboss.SingletonListExternalizer");
      EXTERNALIZERS.put("org.infinispan.transaction.TransactionLog$LogEntry", "org.infinispan.marshall.jboss.TransactionLogExternalizer");
      EXTERNALIZERS.put("org.infinispan.remoting.transport.jgroups.ExtendedResponse", "org.infinispan.marshall.jboss.ExtendedResponseExternalizer");
      EXTERNALIZERS.put("org.infinispan.atomic.AtomicHashMap", "org.infinispan.marshall.jboss.DeltaAwareExternalizer");

      EXTERNALIZERS.put("org.infinispan.commands.control.StateTransferControlCommand", "org.infinispan.marshall.jboss.StateTransferControlCommandExternalizer");
      EXTERNALIZERS.put("org.infinispan.commands.remote.ClusteredGetCommand", "org.infinispan.marshall.jboss.ReplicableCommandExternalizer");
      EXTERNALIZERS.put("org.infinispan.commands.remote.MultipleRpcCommand", "org.infinispan.marshall.jboss.ReplicableCommandExternalizer");
      EXTERNALIZERS.put("org.infinispan.commands.remote.SingleRpcCommand", "org.infinispan.marshall.jboss.ReplicableCommandExternalizer");      
      EXTERNALIZERS.put("org.infinispan.commands.read.GetKeyValueCommand", "org.infinispan.marshall.jboss.ReplicableCommandExternalizer");
      EXTERNALIZERS.put("org.infinispan.commands.write.PutKeyValueCommand", "org.infinispan.marshall.jboss.ReplicableCommandExternalizer");
      EXTERNALIZERS.put("org.infinispan.commands.write.RemoveCommand", "org.infinispan.marshall.jboss.ReplicableCommandExternalizer");
      EXTERNALIZERS.put("org.infinispan.commands.write.InvalidateCommand", "org.infinispan.marshall.jboss.ReplicableCommandExternalizer");
      EXTERNALIZERS.put("org.infinispan.commands.write.ReplaceCommand", "org.infinispan.marshall.jboss.ReplicableCommandExternalizer");
      EXTERNALIZERS.put("org.infinispan.commands.write.ClearCommand", "org.infinispan.marshall.jboss.ReplicableCommandExternalizer");
      EXTERNALIZERS.put("org.infinispan.commands.write.PutMapCommand", "org.infinispan.marshall.jboss.ReplicableCommandExternalizer");
      EXTERNALIZERS.put("org.infinispan.commands.tx.PrepareCommand", "org.infinispan.marshall.jboss.ReplicableCommandExternalizer");
      EXTERNALIZERS.put("org.infinispan.commands.tx.CommitCommand", "org.infinispan.marshall.jboss.ReplicableCommandExternalizer");
      EXTERNALIZERS.put("org.infinispan.commands.tx.RollbackCommand", "org.infinispan.marshall.jboss.ReplicableCommandExternalizer");

      EXTERNALIZERS.put("org.infinispan.container.entries.ImmortalCacheEntry", "org.infinispan.marshall.jboss.InternalCachedEntryExternalizer");
      EXTERNALIZERS.put("org.infinispan.container.entries.MortalCacheEntry", "org.infinispan.marshall.jboss.InternalCachedEntryExternalizer");
      EXTERNALIZERS.put("org.infinispan.container.entries.TransientCacheEntry", "org.infinispan.marshall.jboss.InternalCachedEntryExternalizer");
      EXTERNALIZERS.put("org.infinispan.container.entries.TransientMortalCacheEntry", "org.infinispan.marshall.jboss.InternalCachedEntryExternalizer");
   }
   
   private final Map<Class<?>, Externalizer> externalizers = new WeakHashMap<Class<?>, Externalizer>();
   private final RpcManager rpcManager;
   private final CustomObjectTable objectTable;
   
   public ExternalizerClassFactory(RpcManager rpcManager, CustomObjectTable objectTable) {
      this.rpcManager = rpcManager;
      this.objectTable = objectTable;
   }
   
   public void init() {
	try {
	   for (Map.Entry<String, String> entry : EXTERNALIZERS.entrySet()) {
		Class typeClazz = Util.loadClass(entry.getKey());
		Externalizer ext = (Externalizer) Util.getInstance(entry.getValue());
		if (ext instanceof StateTransferControlCommandExternalizer) {
		   ((StateTransferControlCommandExternalizer) ext).init(rpcManager);
		}
		externalizers.put(typeClazz, ext);
		objectTable.add(ext);
	   }
	} catch (IOException e) {
	   throw new CacheException("Unable to open load magicnumbers.properties", e);
	} catch (ClassNotFoundException e) {
	   throw new CacheException("Unable to load one of the classes defined in the magicnumbers.properties", e);
	} catch (Exception e) {
	   throw new CacheException("Unable to instantiate Externalizer class", e);
	}	
   }
   
   public void stop() {
      externalizers.clear();
   }

   public Externalizer getExternalizer(Class<?> clazz) {
      return externalizers.get(clazz);
   }
}
