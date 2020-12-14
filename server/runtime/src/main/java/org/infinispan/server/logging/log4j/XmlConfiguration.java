package org.infinispan.server.logging.log4j;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.AbstractConfiguration;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.Node;
import org.apache.logging.log4j.core.config.Reconfigurable;
import org.apache.logging.log4j.core.config.plugins.util.PluginType;
import org.apache.logging.log4j.core.config.plugins.util.ResolverUtil;
import org.apache.logging.log4j.core.config.status.StatusConfiguration;
import org.apache.logging.log4j.core.util.Patterns;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.io.ConfigurationReader;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.parsing.ParseUtils;

public class XmlConfiguration extends AbstractConfiguration implements Reconfigurable {

   private static final String[] VERBOSE_CLASSES = new String[]{ResolverUtil.class.getName()};

   private final List<Status> status = new ArrayList<>();

   public XmlConfiguration(final LoggerContext loggerContext, final ConfigurationSource configSource) {
      super(loggerContext, configSource);
   }

   private void parseConfiguration(ConfigurationReader reader) {
      StatusConfiguration statusConfig = new StatusConfiguration().withVerboseClasses(VERBOSE_CLASSES)
            .withStatus(getDefaultStatus());
      int monitorIntervalSeconds = 0;
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         switch (reader.getAttributeName(i).toLowerCase()) {
            case "status":
               statusConfig.withStatus(value);
               break;
            case "dest":
               statusConfig.withDestination(value);
               break;
            case "shutdownhook":
               isShutdownHookEnabled = !"disable".equals(value);
               break;
            case "shutdowntimeout":
               shutdownTimeoutMillis = Long.parseLong(value);
               break;
            case "verbose":
               statusConfig.withVerbosity(value);
               break;
            case "packages":
               pluginPackages.addAll(Arrays.asList(value.split(Patterns.COMMA_SEPARATOR)));
               break;
            case "name":
               setName(value);
               break;
            case "strict":
            case "schema":
               break;
            case "monitorinterval":
               monitorIntervalSeconds = Integer.parseInt(value);
               break;
            case "advertiser":
               //createAdvertiser(value, getConfigurationSource(), buffer, "text/xml");
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      initializeWatchers(this, getConfigurationSource(), monitorIntervalSeconds);
      statusConfig.initialize();
      if (getName() == null) {
         setName(getConfigurationSource().getLocation());
      }
      parseElement(reader, rootNode);
   }

   @Override
   public void setup() {
      final File configFile = getConfigurationSource().getFile();
      try (InputStream configStream = Files.newInputStream(configFile.toPath())) {
         ConfigurationReader reader = ConfigurationReader.from(configStream).withProperties(System.getProperties()).withReplacer((string, props) -> getStrSubstitutor().replace(string)).withType(MediaType.APPLICATION_XML).build();
         reader.require(ConfigurationReader.ElementType.START_DOCUMENT);
         reader.nextElement();
         reader.require(ConfigurationReader.ElementType.START_ELEMENT, null, "Configuration");
         parseConfiguration(reader);
         while (reader.nextElement() != ConfigurationReader.ElementType.END_DOCUMENT) {
            // consume remaining parsing events
         }
      } catch (IOException e) {
         throw new CacheConfigurationException(e);
      }
      if (status.size() > 0) {
         for (final Status s : status) {
            LOGGER.error("Error processing element {} ({}): {}", s.name, s.element, s.errorType);
         }
         return;
      }
   }

   @Override
   public Configuration reconfigure() {
      try {
         final ConfigurationSource source = getConfigurationSource().resetInputStream();
         if (source == null) {
            return null;
         }
         return new XmlConfiguration(getLoggerContext(), source);
      } catch (final IOException ex) {
         LOGGER.error("Cannot locate file {}", getConfigurationSource(), ex);
      }
      return null;
   }

   private void parseElement(ConfigurationReader reader, Node node) {
      processAttributes(reader, node);
      final List<Node> children = node.getChildren();
      while (reader.inTag()) {
         String name = reader.getLocalName();
         PluginType<?> type = pluginManager.getPluginType(name);
         Node childNode = new Node(node, name, type);
         switch (name.toLowerCase()) {
            case "property":
               processAttributes(reader, childNode);
               childNode.setValue(reader.getElementText().trim());
               children.add(childNode);
               break;
            default:
               parseElement(reader, childNode);
               if (type == null) {
                  final String value = childNode.getValue();
                  if (!childNode.hasChildren() && value != null) {
                     node.getAttributes().put(name, value);
                  } else {
                     status.add(new Status(name, name, ErrorType.CLASS_NOT_FOUND));
                  }
               } else {
                  children.add(childNode);
               }
         }
      }
   }

   private void processAttributes(ConfigurationReader reader, Node node) {
      final Map<String, String> attributes = node.getAttributes();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         attributes.put(reader.getAttributeName(i), reader.getAttributeValue(i));
      }
   }

   /**
    * The error that occurred.
    */
   private enum ErrorType {
      CLASS_NOT_FOUND
   }

   /**
    * Status for recording errors.
    */
   private static class Status {
      private final String element;
      private final String name;
      private final ErrorType errorType;

      public Status(final String name, final String element, final ErrorType errorType) {
         this.name = name;
         this.element = element;
         this.errorType = errorType;
      }

      @Override
      public String toString() {
         return "Status [name=" + name + ", element=" + element + ", errorType=" + errorType + "]";
      }

   }

}
