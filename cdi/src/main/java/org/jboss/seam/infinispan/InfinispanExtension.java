package org.jboss.seam.infinispan;

import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import javax.enterprise.event.Observes;
import javax.enterprise.inject.spi.AnnotatedMember;
import javax.enterprise.inject.spi.AnnotatedType;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.BeforeBeanDiscovery;
import javax.enterprise.inject.spi.Extension;
import javax.enterprise.inject.spi.ProcessProducer;
import javax.enterprise.inject.spi.Producer;

import org.infinispan.config.Configuration;
import org.infinispan.manager.CacheContainer;
import org.jboss.seam.solder.bean.Beans;
import org.jboss.seam.solder.literal.GenericTypeLiteral;
import org.jboss.seam.solder.reflection.annotated.AnnotatedTypeBuilder;

public class InfinispanExtension implements Extension {

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

   private final Collection<ConfigurationHolder> configurations;

   InfinispanExtension() {
      this.configurations = new HashSet<InfinispanExtension.ConfigurationHolder>();
   }

   void registerConfiguration(@Observes BeforeBeanDiscovery event,
         BeanManager beanManager) {
      event.addAnnotatedType(makeGeneric(Configuration.class));
      event.addAnnotatedType(makeGeneric(CacheContainer.class));
   }

   private <X> AnnotatedType<X> makeGeneric(Class<X> clazz) {
      AnnotatedTypeBuilder<X> builder = new AnnotatedTypeBuilder<X>()
            .readFromType(clazz);
      builder.addToClass(new GenericTypeLiteral(Infinispan.class));
      return builder.create();
   }

   void observeConfigurationProducer(
         @Observes ProcessProducer<?, Configuration> event,
         BeanManager beanManager) {
      if (event.getAnnotatedMember().isAnnotationPresent(Infinispan.class)) {
         // This is generic configuration, so auto-register it
         String name = event.getAnnotatedMember()
               .getAnnotation(Infinispan.class).value();
         configurations.add(new ConfigurationHolder(event.getProducer(), name,
               event.getAnnotatedMember(), beanManager));
      }
   }

   Collection<ConfigurationHolder> getConfigurations() {
      return configurations;
   }

}
