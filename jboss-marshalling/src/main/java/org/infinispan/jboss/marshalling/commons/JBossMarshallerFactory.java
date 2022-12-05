package org.infinispan.jboss.marshalling.commons;

import java.io.IOException;

import org.jboss.marshalling.AbstractMarshallerFactory;
import org.jboss.marshalling.Marshalling;
import org.jboss.marshalling.MarshallingConfiguration;
import org.jboss.marshalling.reflect.SerializableClassRegistry;
import org.jboss.marshalling.river.RiverMarshallerFactory;

/**
 * A JBoss Marshalling factory class for retrieving marshaller/unmarshaller
 * instances. The aim of this factory is to allow Infinispan to provide its own
 * JBoss Marshalling marshaller/unmarshaller extensions.
 *
 * @author Galder Zamarreño
 * @since 5.1
 * @deprecated since 11.0. To be removed in 14.0 ISPN-11947.
 */
@Deprecated
public class JBossMarshallerFactory extends AbstractMarshallerFactory {

   private final SerializableClassRegistry registry;

   private final RiverMarshallerFactory factory;

   public JBossMarshallerFactory() {
      factory = (RiverMarshallerFactory) Marshalling.getMarshallerFactory(
            "river", Marshalling.class.getClassLoader());
      if (factory == null)
         throw new IllegalStateException(
            "River marshaller factory not found.  Verify that the JBoss Marshalling River jar archive is in the classpath.");
      registry = SerializableClassRegistry.getInstance();
   }

   @Override
   public ExtendedRiverUnmarshaller createUnmarshaller(MarshallingConfiguration configuration) throws IOException {
      return new ExtendedRiverUnmarshaller(factory, registry, configuration);
   }

   @Override
   public ExtendedRiverMarshaller createMarshaller(MarshallingConfiguration configuration) throws IOException {
      return new ExtendedRiverMarshaller(factory, registry, configuration);
   }

}
