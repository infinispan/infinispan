/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other
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
 * 
 */
package org.infinispan.loaders.jpa;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * 
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 *
 */
public class EntityManagerFactoryRegistry {
   private static final Log log = LogFactory.getLog(EntityManagerFactoryRegistry.class);
   
	private Map<String, EntityManagerFactory> registry = new HashMap<String, EntityManagerFactory>();
	public Map<String, AtomicInteger> usage =  new HashMap<String, AtomicInteger>();
	
	public EntityManagerFactory getEntityManagerFactory(String persistenceUnitName) {
		synchronized (this) {
			if (!registry.containsKey(persistenceUnitName)) {
				EntityManagerFactory emf = Persistence.createEntityManagerFactory(persistenceUnitName);
				registry.put(persistenceUnitName, emf);
				usage.put(persistenceUnitName, new AtomicInteger(1));
				return emf;
			} else {
			   incrementUsage(persistenceUnitName);
			   return registry.get(persistenceUnitName);
			}
		}
	}
	
	public void closeEntityManagerFactory(String persistenceUnitName) {
		synchronized (this) {
			if (!registry.containsKey(persistenceUnitName)) {
				return;
			}
			
			int count = decrementUsage(persistenceUnitName);
			if (count == 0) {
				EntityManagerFactory emf = registry.remove(persistenceUnitName);
				try {
				   if (emf.isOpen()) emf.close();
				} catch (IllegalStateException e) {
				   log.warn("Entity manager factory was already closed: " + persistenceUnitName);
				}
			}
		}
	}
	
	public void closeAll() {
	   synchronized (this) {
   	   for (Entry<String, EntityManagerFactory> entry : registry.entrySet()) {
   	      try {
   	         if (entry.getValue().isOpen())
   	            entry.getValue().close();
   	      } catch (IllegalStateException e) {
   	         log.warn("Entity manager factory was already closed: " + entry.getKey());
   	      }
   	   }
	   }
	}
	
	protected int incrementUsage(String persistenceUnitName) {
		synchronized (this) {
			return usage.get(persistenceUnitName).incrementAndGet();
		}
	}
	
	protected int decrementUsage(String persistenceUnitName) {
		synchronized (this) {
			return usage.get(persistenceUnitName).decrementAndGet();
		}
	}
	
	protected int getUsage(String persistenceUnitName) {
		synchronized (this) {
			return usage.get(persistenceUnitName).intValue();
		}
	}
}
