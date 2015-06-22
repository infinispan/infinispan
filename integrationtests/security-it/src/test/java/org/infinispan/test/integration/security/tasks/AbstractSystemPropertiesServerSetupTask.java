package org.infinispan.test.integration.security.tasks;

import org.jboss.as.arquillian.api.ServerSetupTask;
import org.jboss.as.arquillian.container.ManagementClient;
import org.jboss.as.controller.descriptions.ModelDescriptionConstants;
import org.jboss.as.test.integration.security.common.CoreUtils;
import org.jboss.dmr.ModelNode;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.*;

/**
 * {@link ServerSetupTask} instance for system properties setup.
 *
 * @author <a href="mailto:jcacek@redhat.com">Josef Cacek</a>
 * @author <a href="mailto:vchepeli@redhat.com">Vitalii Chepeliuk</a>
 */
public abstract class AbstractSystemPropertiesServerSetupTask implements ServerSetupTask {

   private static final Logger LOGGER = Logger.getLogger(AbstractSystemPropertiesServerSetupTask.class);

   private SystemProperty[] systemProperties;

   public void setup(final ManagementClient managementClient, String containerId) throws Exception {
      if (LOGGER.isInfoEnabled()) {
         LOGGER.info("Adding system properties.");
      }
      systemProperties = getSystemProperties();
      if (systemProperties == null || systemProperties.length == 0) {
         LOGGER.warn("No system property configured in the ServerSetupTask");
         return;
      }

      final List<ModelNode> updates = new ArrayList<ModelNode>();

      for (SystemProperty systemProperty : systemProperties) {
         final String propertyName = systemProperty.getName();
         if (propertyName == null || propertyName.trim().length() == 0) {
            LOGGER.warn("Empty property name provided.");
            continue;
         }
         ModelNode op = new ModelNode();
         op.get(OP).set(ADD);
         op.get(OP_ADDR).add(SYSTEM_PROPERTY, propertyName);
         op.get(ModelDescriptionConstants.VALUE).set(systemProperty.getValue());
         updates.add(op);
      }
      CoreUtils.applyUpdates(updates, managementClient.getControllerClient());
   }

   public void tearDown(ManagementClient managementClient, String containerId) throws Exception {
      if (LOGGER.isInfoEnabled()) {
         LOGGER.info("Removing system properties.");
      }
      if (systemProperties == null || systemProperties.length == 0) {
         return;
      }

      final List<ModelNode> updates = new ArrayList<ModelNode>();

      for (SystemProperty systemProperty : systemProperties) {
         final String propertyName = systemProperty.getName();
         if (propertyName == null || propertyName.trim().length() == 0) {
            continue;
         }
         ModelNode op = new ModelNode();
         op.get(OP).set(REMOVE);
         op.get(OP_ADDR).add(SYSTEM_PROPERTY, propertyName);
         updates.add(op);
      }
      CoreUtils.applyUpdates(updates, managementClient.getControllerClient());

   }

   public static SystemProperty[] mapToSystemProperties(Map<String, String> map) {
      if (map == null || map.isEmpty()) {
         return null;
      }
      final List<SystemProperty> list = new ArrayList<SystemProperty>();
      for (Map.Entry<String, String> property : map.entrySet()) {
         list.add(new DefaultSystemProperty(property.getKey(), property.getValue()));
      }
      return list.toArray(new SystemProperty[list.size()]);
   }

   public interface SystemProperty {
      String getName();

      String getValue();

   }

   public static class DefaultSystemProperty implements SystemProperty {
      private final String name;
      private final String value;

      /**
       * Create a new DefaultSystemProperty.
       *
       * @param name
       * @param value
       */
      public DefaultSystemProperty(String name, String value) {
         super();
         this.name = name;
         this.value = value;
      }

      /**
       * Get the name.
       *
       * @return the name.
       */
      public String getName() {
         return name;
      }

      /**
       * Get the value.
       *
       * @return the value.
       */
      public String getValue() {
         return value;
      }

   }

   protected abstract SystemProperty[] getSystemProperties();
}