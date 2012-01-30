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
package org.infinispan.marshall.jboss;

import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.marshall.StreamingMarshaller;
import org.jboss.marshalling.Marshaller;
import org.jboss.marshalling.MarshallingConfiguration;
import org.jboss.marshalling.Unmarshaller;

import java.io.IOException;

/**
 * A JBoss Marshalling based marshaller that is oriented at internal, embedded,
 * Infinispan usage. It uses of a custom object table for Infinispan based
 * Externalizer instances that are either internal or user defined.
 * <p />
 * The reason why this is implemented specially in Infinispan rather than resorting to Java serialization or even the
 * more efficient JBoss serialization is that a lot of efficiency can be gained when a majority of the serialization
 * that occurs has to do with a small set of known types such as {@link org.infinispan.transaction.xa.GlobalTransaction} or
 * {@link org.infinispan.commands.ReplicableCommand}, and class type information can be replaced with simple magic
 * numbers.
 * <p/>
 * Unknown types (typically user data) falls back to Java serialization.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
public class JBossMarshaller extends AbstractJBossMarshaller implements StreamingMarshaller {

   ExternalizerTable externalizerTable;

   private final ThreadLocal<PerThreadInstanceHolder> marshallerTL = new ThreadLocal<PerThreadInstanceHolder>() {
      @Override
      protected PerThreadInstanceHolder initialValue() {
         Marshaller marshaller = null;
         try {
            marshaller = factory.createMarshaller(baseCfg);
         } catch (IOException e) {
            log.error("Could not create Marshaller to pool. Performance will be reduced!", e);
            return null;
         }
         MarshallingConfiguration cfg = baseCfg.clone();
         return new PerThreadInstanceHolder(cfg, marshaller);
      }
   };

   public void inject(ExternalizerTable externalizerTable, ClassLoader cl, InvocationContextContainer icc) {
      log.debug("Using JBoss Marshalling");
      this.externalizerTable = externalizerTable;
      baseCfg.setObjectTable(externalizerTable);
      // Override the class resolver with one that can detect injected
      // classloaders via AdvancedCache.with(ClassLoader) calls.
      baseCfg.setClassResolver(new EmbeddedContextClassResolver(cl, icc));
   }

   @Override
   protected Marshaller getMarshaller(boolean isReentrant, final int estimatedSize) throws IOException {
      PerThreadInstanceHolder instanceHolder = marshallerTL.get();
      if (instanceHolder == null) {
         //not expected to happen: only for the case we get an IOException in the #initialValue of the threadlocal
         //slowest option:
         return factory.createMarshaller(baseCfg);
      }
      if (isReentrant) {
         MarshallingConfiguration customConfiguration = instanceHolder.threadDedicatedConfiguration;
         customConfiguration.setBufferSize(estimatedSize);
         return factory.createMarshaller(customConfiguration);
      }
      else {
         return instanceHolder.reusableMarshaller;
      }
   }

   @Override
   protected Unmarshaller getUnmarshaller(boolean isReentrant) throws IOException {
      return factory.createUnmarshaller(baseCfg);
   }

   @Override
   public void stop() {
      super.stop();
      // Just in case, to avoid leaking class resolver which references classloader
      baseCfg.setClassResolver(null);
   }

   @Override
   public boolean isMarshallableCandidate(Object o) {
      return super.isMarshallableCandidate(o) || externalizerTable.isMarshallableCandidate(o);
   }

   /**
    * An embedded context class resolver that is able to retrieve a class
    * loader from the embedded Infinispan call context. This might happen when
    * {@link org.infinispan.AdvancedCache#with(ClassLoader)} is used.
    */
   public static final class EmbeddedContextClassResolver extends DefaultContextClassResolver {

      private final InvocationContextContainer icc;

      public EmbeddedContextClassResolver(ClassLoader defaultClassLoader, InvocationContextContainer icc) {
         super(defaultClassLoader);
         this.icc = icc;
      }

      @Override
      protected ClassLoader getClassLoader() {
         if (icc != null) {
            InvocationContext ctx = icc.getInvocationContext(true);
            if (ctx != null) {
               ClassLoader cl = ctx.getClassLoader();
               if (cl != null) return cl;
            }
         }
         return super.getClassLoader();
      }
   }

   public static final class PerThreadInstanceHolder {
      final MarshallingConfiguration threadDedicatedConfiguration;
      final Marshaller reusableMarshaller;
      PerThreadInstanceHolder(final MarshallingConfiguration threadDedicatedConfiguration, final Marshaller reusableMarshaller) {
         this.threadDedicatedConfiguration = threadDedicatedConfiguration;
         this.reusableMarshaller = reusableMarshaller;
      }
   }

}
