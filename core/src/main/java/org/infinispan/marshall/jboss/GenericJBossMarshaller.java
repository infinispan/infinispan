/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.marshall.jboss;

import org.infinispan.CacheException;
import org.jboss.marshalling.Marshaller;
import org.jboss.marshalling.Unmarshaller;

import java.io.IOException;

/**
 * A marshaller that makes use of <a href="http://www.jboss.org/jbossmarshalling">JBoss Marshalling</a>
 * to serialize and deserialize objects. This marshaller is oriented at external,
 * non-core Infinispan use, such as the Java Hot Rod client.
 *
 * @author Manik Surtani
 * @version 4.1
 * @see <a href="http://www.jboss.org/jbossmarshalling">JBoss Marshalling</a>
 */
public class GenericJBossMarshaller extends AbstractJBossMarshaller {

   /**
    * Marshaller thread local. In non-internal marshaller usages, such as Java
    * Hot Rod client, this is a singleton shared by all so no urgent need for
    * static here. JBMAR clears pretty much any state during finish(), so no
    * urgent need to clear the thread local since it shouldn't be leaking.
    */
   private ThreadLocal<org.jboss.marshalling.Marshaller> marshallerTL =
         new ThreadLocal<org.jboss.marshalling.Marshaller>() {
      @Override
      protected org.jboss.marshalling.Marshaller initialValue() {
         try {
            return factory.createMarshaller(baseCfg);
         } catch (IOException e) {
            throw new CacheException(e);
         }
      }
   };

   /**
    * Unmarshaller thread local. In non-internal marshaller usages, such as
    * Java Hot Rod client, this is a singleton shared by all so no urgent need
    * for static here. JBMAR clears pretty much any state during finish(), so
    * no urgent need to clear the thread local since it shouldn't be leaking.
    */
   private ThreadLocal<Unmarshaller> unmarshallerTL = new
         ThreadLocal<Unmarshaller>() {
      @Override
      protected Unmarshaller initialValue() {
         try {
            return factory.createUnmarshaller(baseCfg);
         } catch (IOException e) {
            throw new CacheException(e);
         }
      }
   };

   public GenericJBossMarshaller() {
      super();
      baseCfg.setClassResolver(
            new DefaultContextClassResolver(this.getClass().getClassLoader()));
   }

   protected Marshaller getMarshaller(boolean isReentrant) throws IOException {
      Marshaller marshaller = isReentrant ?
            factory.createMarshaller(baseCfg) : marshallerTL.get();

      if (log.isTraceEnabled())
         log.tracef("Start marshaller after retrieving marshaller from %s",
                   isReentrant ? "factory" : "thread local");

      return marshaller;
   }

   protected Unmarshaller getUnmarshaller(boolean isReentrant) throws IOException {
      Unmarshaller unmarshaller = isReentrant ?
            factory.createUnmarshaller(baseCfg) : unmarshallerTL.get();

      if (log.isTraceEnabled())
         log.tracef("Start unmarshaller after retrieving marshaller from %s",
                   isReentrant ? "factory" : "thread local");

      return unmarshaller;
   }

}
