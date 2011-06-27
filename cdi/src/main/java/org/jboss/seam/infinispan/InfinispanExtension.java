/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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
package org.jboss.seam.infinispan;

import org.infinispan.config.Configuration;
import org.infinispan.manager.CacheContainer;
import org.jboss.seam.solder.bean.Beans;
import org.jboss.seam.solder.literal.GenericTypeLiteral;
import org.jboss.seam.solder.reflection.annotated.AnnotatedTypeBuilder;

import javax.enterprise.event.Observes;
import javax.enterprise.inject.spi.AnnotatedMember;
import javax.enterprise.inject.spi.AnnotatedType;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.BeforeBeanDiscovery;
import javax.enterprise.inject.spi.Extension;
import javax.enterprise.inject.spi.ProcessProducer;
import javax.enterprise.inject.spi.Producer;
import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class InfinispanExtension implements Extension {

   private final Collection<ConfigurationHolder> configurations;

   InfinispanExtension() {
      this.configurations = new HashSet<InfinispanExtension.ConfigurationHolder>();
   }

   Collection<ConfigurationHolder> getConfigurations() {
      return configurations;
   }

   void registerConfiguration(@Observes BeforeBeanDiscovery event) {
      event.addAnnotatedType(makeGeneric(Configuration.class));
      event.addAnnotatedType(makeGeneric(CacheContainer.class));
   }

   void observeConfigurationProducer(@Observes ProcessProducer<?, Configuration> event, BeanManager beanManager) {
      Infinispan annotation = event.getAnnotatedMember().getAnnotation(Infinispan.class);
      if (annotation != null) {
         // This is generic configuration, so auto-register it
         String name = annotation.value();
         configurations.add(new ConfigurationHolder(
               event.getProducer(),
               name,
               event.getAnnotatedMember(),
               beanManager
         ));
      }
   }

   private <X> AnnotatedType<X> makeGeneric(Class<X> clazz) {
      return new AnnotatedTypeBuilder<X>()
            .readFromType(clazz)
            .addToClass(new GenericTypeLiteral(Infinispan.class))
            .create();
   }

   static class ConfigurationHolder {

      private final Producer<Configuration> producer;
      private final Set<Annotation> qualifiers;
      private final String name;

      ConfigurationHolder(Producer<Configuration> producer, String name,
            AnnotatedMember<?> annotatedMember, BeanManager beanManager) {
         this.producer = producer;
         this.name = name;
         this.qualifiers = Beans.getQualifiers(beanManager,
               annotatedMember.getAnnotations());
      }

      Producer<Configuration> getProducer() {
         return producer;
      }

      String getName() {
         return name;
      }

      public Set<Annotation> getQualifiers() {
         return qualifiers;
      }

   }
}
