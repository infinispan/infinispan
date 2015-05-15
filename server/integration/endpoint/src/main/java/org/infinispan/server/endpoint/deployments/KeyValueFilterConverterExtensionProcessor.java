package org.infinispan.server.endpoint.deployments;

import org.infinispan.filter.KeyValueFilterConverterFactory;
import static org.infinispan.server.endpoint.EndpointLogger.ROOT_LOGGER;
import org.jboss.msc.service.ServiceName;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StopContext;

/**
 * @author gustavonalle
 * @since 8.0
 */
public class KeyValueFilterConverterExtensionProcessor extends AbstractNamedFactoryExtensionProcessor<KeyValueFilterConverterFactory> {

   public KeyValueFilterConverterExtensionProcessor(ServiceName extensionManagerServiceName) {
      super(extensionManagerServiceName);
   }

   @Override
   public AbstractExtensionManagerService<KeyValueFilterConverterFactory> createService(String name, KeyValueFilterConverterFactory instance) {
      return new KeyValueFilterConverterFactoryService(name, instance);
   }

   @Override
   public Class<KeyValueFilterConverterFactory> getServiceClass() {
      return KeyValueFilterConverterFactory.class;
   }

   private static class KeyValueFilterConverterFactoryService extends AbstractExtensionManagerService<KeyValueFilterConverterFactory> {
      private KeyValueFilterConverterFactoryService(String name, KeyValueFilterConverterFactory converterFactory) {
         super(name, converterFactory);
      }

      @Override
      public void start(StartContext context) {
         ROOT_LOGGER.debugf("Started key value filter converter service with name = %s", name);
         extensionManager.getValue().addKeyValueFilterConverterFactory(name, extension);
      }

      @Override
      public void stop(StopContext context) {
         ROOT_LOGGER.debugf("Stopped combined filter and converter service with name = %s", name);
         extensionManager.getValue().removeKeyValueFilterConverterFactory(name);
      }

      @Override
      public KeyValueFilterConverterFactory getValue() {
         return extension;
      }

      @Override
      public String getServiceTypeName() {
         return "key-value-filter-converter-factory";
      }
   }

}
