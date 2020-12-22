package org.infinispan.configuration.parsing;

import static org.infinispan.configuration.parsing.ParseUtils.ignoreAttribute;
import static org.infinispan.configuration.parsing.Parser.NAMESPACE;
import static org.infinispan.factories.KnownComponentNames.ASYNC_NOTIFICATION_EXECUTOR;
import static org.infinispan.factories.KnownComponentNames.BLOCKING_EXECUTOR;
import static org.infinispan.factories.KnownComponentNames.EXPIRATION_SCHEDULED_EXECUTOR;
import static org.infinispan.factories.KnownComponentNames.NON_BLOCKING_EXECUTOR;
import static org.infinispan.factories.KnownComponentNames.shortened;
import static org.infinispan.util.logging.Log.CONFIG;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.AsyncStoreConfigurationBuilder;
import org.infinispan.configuration.cache.StoreConfigurationBuilder;
import org.infinispan.configuration.global.AllowListConfigurationBuilder;
import org.infinispan.configuration.global.GlobalAuthorizationConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.global.GlobalRoleConfigurationBuilder;
import org.infinispan.configuration.global.GlobalStateConfigurationBuilder;
import org.infinispan.configuration.global.SerializationConfigurationBuilder;
import org.infinispan.configuration.global.ShutdownHookBehavior;
import org.infinispan.configuration.global.ThreadPoolBuilderAdapter;
import org.infinispan.configuration.global.ThreadPoolConfiguration;
import org.infinispan.configuration.global.ThreadsConfigurationBuilder;
import org.infinispan.configuration.global.TransportConfigurationBuilder;
import org.infinispan.factories.threads.DefaultThreadFactory;
import org.infinispan.globalstate.ConfigurationStorage;
import org.infinispan.globalstate.LocalConfigurationStorage;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.BuiltinJGroupsChannelConfigurator;
import org.infinispan.remoting.transport.jgroups.EmbeddedJGroupsChannelConfigurator;
import org.infinispan.remoting.transport.jgroups.FileJGroupsChannelConfigurator;
import org.infinispan.remoting.transport.jgroups.JGroupsChannelConfigurator;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.security.PrincipalRoleMapper;
import org.infinispan.security.mappers.ClusterRoleMapper;
import org.infinispan.security.mappers.CommonNameRoleMapper;
import org.infinispan.security.mappers.IdentityRoleMapper;
import org.jgroups.conf.ProtocolConfiguration;
import org.kohsuke.MetaInfServices;

/**
 * This class implements the parser for Infinispan/AS7/EAP/JDG schema files
 *
 * @author Tristan Tarrant
 * @author Galder Zamarreño
 * @since 9.0
 */
@MetaInfServices(ConfigurationParser.class)
@Namespace(root = "infinispan")
@Namespace(uri = NAMESPACE + "*", root = "infinispan")
public class Parser extends CacheParser {

   public static final String NAMESPACE = "urn:infinispan:config:";

   public Parser() {
   }

   @Override
   public void readElement(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder) throws XMLStreamException {
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case CACHE_CONTAINER: {
               parseContainer(reader, holder);
               break;
            }
            case JGROUPS: {
               // Preload some default JGroups stacks
               addJGroupsDefaultStacksIfNeeded(reader, holder);
               parseJGroups(reader, holder);
               break;
            }
            case THREADS: {
               parseThreads(reader, holder);
               break;
            }
            default: {
               reader.handleAny(holder);
               break;
            }
         }
      }
   }

   private void parseSerialization(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder)
         throws XMLStreamException {
      GlobalConfigurationBuilder builder = holder.getGlobalConfigurationBuilder();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));

         switch (attribute) {
            case MARSHALLER_CLASS: {
               builder.serialization().marshaller(Util.getInstance(value, holder.getClassLoader()));
               break;
            }
            case VERSION: {
               if (reader.getSchema().since(11, 0)) {
                  throw ParseUtils.attributeRemoved(reader, i);
               }
               ignoreAttribute(reader, i);
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }

      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case ADVANCED_EXTERNALIZER: {
               CONFIG.advancedExternalizerDeprecated();
               parseAdvancedExternalizer(reader, holder.getClassLoader(), builder.serialization());
               break;
            }
            case SERIALIZATION_CONTEXT_INITIALIZER: {
               parseSerializationContextInitializer(reader, holder.getClassLoader(), builder.serialization());
               break;
            }
            case WHITE_LIST:
               if (reader.getSchema().since(12, 0)) {
                  throw ParseUtils.elementRemoved(reader, Element.ALLOW_LIST.getLocalName());
               }
               CONFIG.elementDeprecatedUseOther(Element.WHITE_LIST, Element.ALLOW_LIST);
               // fall through
            case ALLOW_LIST: {
               if (reader.getSchema().since(10, 0)) {
                  parseAllowList(reader, builder.serialization().allowList());
                  break;
               } else {
                  throw ParseUtils.unexpectedElement(reader);
               }
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
   }

   private void parseSerializationContextInitializer(final XMLExtendedStreamReader reader, final ClassLoader classLoader,
                                                     final SerializationConfigurationBuilder builder) throws XMLStreamException {
      int attributes = reader.getAttributeCount();
      ParseUtils.requireAttributes(reader, Attribute.CLASS.getLocalName());
      for (int i = 0; i < attributes; i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case CLASS: {
               builder.addContextInitializer(Util.getInstance(value, classLoader));
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseAllowList(final XMLExtendedStreamReader reader, final AllowListConfigurationBuilder builder) throws XMLStreamException {
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case CLASS: {
               builder.addClass(reader.getElementText());
               break;
            }
            case REGEX: {
               builder.addRegexp(reader.getElementText());
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
   }

   private void parseAdvancedExternalizer(final XMLExtendedStreamReader reader, final ClassLoader classLoader,
                                          final SerializationConfigurationBuilder builder) throws XMLStreamException {
      int attributes = reader.getAttributeCount();
      AdvancedExternalizer<?> advancedExternalizer = null;
      Integer id = null;
      ParseUtils.requireAttributes(reader, Attribute.CLASS.getLocalName());
      for (int i = 0; i < attributes; i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case CLASS: {
               advancedExternalizer = Util.getInstance(value, classLoader);
               break;
            }
            case ID: {
               id = Integer.valueOf(value);
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }

      ParseUtils.requireNoContent(reader);

      if (id != null) {
         builder.addAdvancedExternalizer(id, advancedExternalizer);
      } else {
         builder.addAdvancedExternalizer(advancedExternalizer);
      }
   }

   private void parseThreads(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case THREAD_FACTORY: {
               parseThreadFactory(reader, holder);
               break;
            }
            case CACHED_THREAD_POOL: {
               parseCachedThreadPool(reader, holder);
               break;
            }
            case SCHEDULED_THREAD_POOL: {
               parseScheduledThreadPool(reader, holder);
               break;
            }
            case BLOCKING_BOUNDED_QUEUE_THREAD_POOL: {
               parseBoundedQueueThreadPool(reader, holder, false);
               break;
            }
            case NON_BLOCKING_BOUNDED_QUEUE_THREAD_POOL: {
               parseBoundedQueueThreadPool(reader, holder, true);
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }

   }

   private void parseBoundedQueueThreadPool(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder,
         boolean isNonBlocking) throws XMLStreamException {
      ThreadsConfigurationBuilder threadsBuilder = holder.getGlobalConfigurationBuilder().threads();

      String name = null;
      String threadFactoryName = null;
      int maxThreads = 0;
      int coreThreads = 0;
      int queueLength = 0;
      long keepAlive = 0;

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));

         switch (attribute) {
            case NAME: {
               name = value;
               break;
            }
            case THREAD_FACTORY: {
               threadFactoryName = value;
               break;
            }
            case CORE_THREADS: {
               coreThreads = Integer.parseInt(value);
               break;
            }
            case MAX_THREADS: {
               maxThreads = Integer.parseInt(value);
               break;
            }
            case QUEUE_LENGTH: {
               queueLength = Integer.parseInt(value);
               break;
            }
            case KEEP_ALIVE_TIME: {
               keepAlive = Long.parseLong(value);
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }

      threadsBuilder.addBoundedThreadPool(name).threadFactory(threadFactoryName).coreThreads(coreThreads)
            .maxThreads(maxThreads).queueLength(queueLength).keepAliveTime(keepAlive).nonBlocking(isNonBlocking);
      ParseUtils.requireNoContent(reader);
   }

   private void parseScheduledThreadPool(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      ThreadsConfigurationBuilder threadsBuilder = holder.getGlobalConfigurationBuilder().threads();
      String name = null;
      String threadFactoryName = null;
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));

         switch (attribute) {
            case NAME: {
               name = value;
               break;
            }
            case THREAD_FACTORY: {
               threadFactoryName = value;
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }

      threadsBuilder.addScheduledThreadPool(name).threadFactory(threadFactoryName);

      ParseUtils.requireNoContent(reader);
   }

   private void parseCachedThreadPool(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      ThreadsConfigurationBuilder threadsBuilder = holder.getGlobalConfigurationBuilder().threads();
      String name = null;
      String threadFactoryName = null;
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));

         switch (attribute) {
            case NAME: {
               name = value;
               break;
            }
            case THREAD_FACTORY: {
               threadFactoryName = value;
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }

      threadsBuilder.addCachedThreadPool(name).threadFactory(threadFactoryName);

      ParseUtils.requireNoContent(reader);
   }

   private void parseThreadFactory(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      String name = null;
      String threadGroupName = null;
      String threadNamePattern = null;
      int priority = 1; // minimum priority

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));

         switch (attribute) {
            case NAME: {
               name = value;
               break;
            }
            case GROUP_NAME: {
               threadGroupName = value;
               break;
            }
            case THREAD_NAME_PATTERN: {
               threadNamePattern = value;
               break;
            }
            case PRIORITY: {
               priority = Integer.parseInt(value);
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }

      holder.getGlobalConfigurationBuilder().threads().addThreadFactory(name).groupName(threadGroupName).priority(priority).threadNamePattern(threadNamePattern);
      ParseUtils.requireNoContent(reader);
   }

   private void parseJGroups(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      Transport transport = null;
      for (int i = 0; i < reader.getAttributeCount(); i++) {
            if (!ParseUtils.isNoNamespaceAttribute(reader, i))
            continue;
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));

         switch (attribute) {
            case TRANSPORT:
               transport = Util.getInstance(value, holder.getClassLoader());
               break;
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }

      if (transport == null) {
         // Set up default transport
         holder.getGlobalConfigurationBuilder().transport().defaultTransport();
      } else {
         holder.getGlobalConfigurationBuilder().transport().transport(transport);
      }

      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case STACK_FILE:
               parseStackFile(reader, holder);
               break;
            case STACK:
               if (!reader.getSchema().since(10, 0)) {
                  throw ParseUtils.unexpectedElement(reader);
               }
               parseJGroupsStack(reader, holder);
               break;
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
   }

   private void addJGroupsStackFile(ConfigurationBuilderHolder holder, String name, String path, Properties properties, XMLResourceResolver resourceResolver) {
      URL url = FileLookupFactory.newInstance().lookupFileLocation(path, holder.getClassLoader());
      try (InputStream xml = (url != null ? url : resourceResolver.resolveResource(path)).openStream()) {
         holder.addJGroupsStack(new FileJGroupsChannelConfigurator(name, path, xml, properties));
      } catch (FileNotFoundException e) {
         throw CONFIG.jgroupsConfigurationNotFound(path);
      } catch (IOException e) {
         throw CONFIG.unableToAddJGroupsStack(name, e);
      }
   }

   private void parseJGroupsStack(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      String stackName = ParseUtils.requireAttributes(reader, Attribute.NAME)[0];
      EmbeddedJGroupsChannelConfigurator stackConfigurator = new EmbeddedJGroupsChannelConfigurator(stackName);
      String extend = null;
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case NAME:
               break;
            case EXTENDS:
               extend = value;
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }

      List<ProtocolConfiguration> stack = stackConfigurator.getProtocolStack();
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case REMOTE_SITES:
               parseJGroupsRelay(reader, holder, stackConfigurator);
               break;
            default:
               // It should be an actual JGroups protocol
               String protocolName = reader.getLocalName();
               Map<String, String> protocolAttributes = new HashMap<>();
               for (int i = 0; i < reader.getAttributeCount(); i++) {
                  protocolAttributes.put(reader.getAttributeLocalName(i), reader.getAttributeValue(i));
               }
               ParseUtils.requireNoContent(reader);
               stack.add(new ProtocolConfiguration(protocolName, protocolAttributes));
               break;
         }
      }
      holder.addJGroupsStack(stackConfigurator, extend);
   }

   private void parseJGroupsRelay(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder, EmbeddedJGroupsChannelConfigurator stackConfigurator) throws XMLStreamException {
      String defaultStack = ParseUtils.requireSingleAttribute(reader, Attribute.DEFAULT_STACK);
      if (holder.getJGroupsStack(defaultStack) == null) {
         throw CONFIG.missingJGroupsStack(defaultStack);
      }
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case REMOTE_SITE:
               String remoteSite = ParseUtils.requireAttributes(reader, Attribute.NAME)[0];
               String stack = defaultStack;
               for (int i = 0; i < reader.getAttributeCount(); i++) {
                  Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
                  switch (attribute) {
                     case NAME:
                        break;
                     case STACK:
                        stack = reader.getAttributeValue(i);
                        if (holder.getJGroupsStack(stack) == null) {
                           throw CONFIG.missingJGroupsStack(stack);
                        }
                        break;
                     default:
                        throw ParseUtils.unexpectedAttribute(reader, i);
                  }
               }
               ParseUtils.requireNoContent(reader);
               stackConfigurator.addRemoteSite(remoteSite, holder.getJGroupsStack(stack));
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private void parseStackFile(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      String[] attributes = ParseUtils.requireAttributes(reader, Attribute.NAME, Attribute.PATH);
      ParseUtils.requireNoContent(reader);

      addJGroupsStackFile(holder, attributes[0], attributes[1], reader.getProperties(), reader.getResourceResolver());
   }

   private void parseContainer(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      holder.pushScope(ParserScope.CACHE_CONTAINER);
      GlobalConfigurationBuilder builder = holder.getGlobalConfigurationBuilder();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         if (!ParseUtils.isNoNamespaceAttribute(reader, i))
            continue;
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case NAME: {
               builder.cacheManagerName(value);
               break;
            }
            case DEFAULT_CACHE: {
               builder.defaultCacheName(value);
               break;
            }
            case ALIASES:
            case JNDI_NAME:
            case MODULE:
            case START:
            case ASYNC_EXECUTOR:
            case PERSISTENCE_EXECUTOR:
            case STATE_TRANSFER_EXECUTOR: {
               if (reader.getSchema().since(11, 0)) {
                  throw ParseUtils.attributeRemoved(reader, i);
               } else {
                  ignoreAttribute(reader, i);
               }
               break;
            }
            case LISTENER_EXECUTOR: {
               builder.listenerThreadPoolName(value);
               builder.listenerThreadPool().read(createThreadPoolConfiguration(value, ASYNC_NOTIFICATION_EXECUTOR, holder));
               break;
            }
            case EVICTION_EXECUTOR:
               if (reader.getSchema().since(11, 0)) {
                  throw ParseUtils.attributeRemoved(reader, i);
               } else {
                  CONFIG.evictionExecutorDeprecated();
               }
               // fallthrough
            case EXPIRATION_EXECUTOR: {
               builder.expirationThreadPoolName(value);
               builder.expirationThreadPool().read(createThreadPoolConfiguration(value, EXPIRATION_SCHEDULED_EXECUTOR, holder));
               break;
            }
            case REPLICATION_QUEUE_EXECUTOR: {
               if (reader.getSchema().since(9, 0)) {
                  throw ParseUtils.attributeRemoved(reader, i);
               } else {
                  CONFIG.ignoredReplicationQueueAttribute(attribute.getLocalName(), reader.getLocation().getLineNumber());
               }
               break;
            }
            case NON_BLOCKING_EXECUTOR: {
               builder.nonBlockingThreadPoolName(value);
               builder.nonBlockingThreadPool().read(createThreadPoolConfiguration(value, NON_BLOCKING_EXECUTOR, holder));
               break;
            }
            case BLOCKING_EXECUTOR: {
               builder.blockingThreadPoolName(value);
               builder.blockingThreadPool().read(createThreadPoolConfiguration(value, BLOCKING_EXECUTOR, holder));
               break;
            }
            case STATISTICS: {
               boolean statistics = Boolean.parseBoolean(value);
               builder.cacheContainer().statistics(statistics);
               if (!reader.getSchema().since(10, 1)) {
                  builder.jmx().enabled(statistics);
               }
               break;
            }
            case SHUTDOWN_HOOK: {
               builder.shutdown().hookBehavior(ShutdownHookBehavior.valueOf(value));
               break;
            }
            case ZERO_CAPACITY_NODE: {
               builder.zeroCapacityNode(Boolean.parseBoolean(value));
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }

      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case TRANSPORT: {
               parseTransport(reader, holder);
               break;
            }
            case LOCAL_CACHE: {
               parseLocalCache(reader, holder, false);
               break;
            }
            case LOCAL_CACHE_CONFIGURATION: {
               parseLocalCache(reader, holder, true);
               break;
            }
            case INVALIDATION_CACHE: {
               parseInvalidationCache(reader, holder, false);
               break;
            }
            case INVALIDATION_CACHE_CONFIGURATION: {
               parseInvalidationCache(reader, holder, true);
               break;
            }
            case REPLICATED_CACHE: {
               parseReplicatedCache(reader, holder, false);
               break;
            }
            case REPLICATED_CACHE_CONFIGURATION: {
               parseReplicatedCache(reader, holder, true);
               break;
            }
            case DISTRIBUTED_CACHE: {
               parseDistributedCache(reader, holder, false);
               break;
            }
            case DISTRIBUTED_CACHE_CONFIGURATION: {
               parseDistributedCache(reader, holder, true);
               break;
            }
            case SCATTERED_CACHE: {
               if (reader.getSchema().since(9, 1)) {
                  parseScatteredCache(reader, holder, false);
               } else {
                  throw ParseUtils.unexpectedElement(reader);
               }
               break;
            }
            case SCATTERED_CACHE_CONFIGURATION: {
               if (reader.getSchema().since(9, 1)) {
                  parseScatteredCache(reader, holder, true);
               } else {
                  throw ParseUtils.unexpectedElement(reader);
               }
               break;
            }
            case SERIALIZATION: {
               parseSerialization(reader, holder);
               break;
            }
            case MODULES: {
               if (reader.getSchema().since(9, 0)) {
                  throw ParseUtils.elementRemoved(reader);
               } else {
                  parseModules(reader, holder);
               }
               break;
            }
            case METRICS: {
               parseMetrics(reader, holder);
               break;
            }
            case JMX: {
               parseJmx(reader, holder);
               break;
            }
            case SECURITY: {
               parseGlobalSecurity(reader, holder);
               break;
            }
            case GLOBAL_STATE: {
               if (reader.getSchema().since(8, 1)) {
                  parseGlobalState(reader, holder);
               } else {
                  throw ParseUtils.unexpectedElement(reader);
               }
               break;
            }
            default: {
               reader.handleAny(holder);
            }
         }
      }
      holder.popScope();
   }

   private void parseGlobalSecurity(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      ParseUtils.requireNoAttributes(reader);
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case AUTHORIZATION: {
               parseGlobalAuthorization(reader, holder);
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
   }

   private void parseGlobalAuthorization(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      GlobalAuthorizationConfigurationBuilder builder = holder.getGlobalConfigurationBuilder().security().authorization().enable();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case AUDIT_LOGGER: {
               builder.auditLogger(Util.getInstance(value, holder.getClassLoader()));
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
      PrincipalRoleMapper roleMapper = null;
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case IDENTITY_ROLE_MAPPER:
               if (roleMapper != null) {
                  throw ParseUtils.unexpectedElement(reader);
               }
               ParseUtils.requireNoAttributes(reader);
               ParseUtils.requireNoContent(reader);
               roleMapper = new IdentityRoleMapper();
               break;
            case COMMON_NAME_ROLE_MAPPER:
               if (roleMapper != null) {
                  throw ParseUtils.unexpectedElement(reader);
               }
               ParseUtils.requireNoAttributes(reader);
               ParseUtils.requireNoContent(reader);
               roleMapper = new CommonNameRoleMapper();
               break;
            case CLUSTER_ROLE_MAPPER:
               if (roleMapper != null) {
                  throw ParseUtils.unexpectedElement(reader);
               }
               ParseUtils.requireNoAttributes(reader);
               ParseUtils.requireNoContent(reader);
               roleMapper = new ClusterRoleMapper();
               break;
            case CUSTOM_ROLE_MAPPER:
               if (roleMapper != null) {
                  throw ParseUtils.unexpectedElement(reader);
               }
               roleMapper = parseCustomMapper(reader, holder);
               break;
            case ROLE: {
               parseGlobalRole(reader, builder);
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
      if (roleMapper != null) {
         builder.principalRoleMapper(roleMapper);
      }
   }

   private PrincipalRoleMapper parseCustomMapper(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      String mapperClass = ParseUtils.requireSingleAttribute(reader, Attribute.CLASS.getLocalName());
      ParseUtils.requireNoContent(reader);
      return Util.getInstance(mapperClass, holder.getClassLoader());
   }

   private void parseGlobalRole(XMLExtendedStreamReader reader, GlobalAuthorizationConfigurationBuilder builder) throws XMLStreamException {
      String[] attributes = ParseUtils.requireAttributes(reader, Attribute.NAME.getLocalName(), Attribute.PERMISSIONS.getLocalName());
      GlobalRoleConfigurationBuilder role = builder.role(attributes[0]);
      for(String permission : attributes[1].split("\\s+")) {
         role.permission(permission);
      }
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case NAME:
            case PERMISSIONS: {
               // Already handled
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseMetrics(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      GlobalConfigurationBuilder builder = holder.getGlobalConfigurationBuilder();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case GAUGES: {
               builder.metrics().gauges(Boolean.parseBoolean(value));
               break;
            }
            case HISTOGRAMS: {
               builder.metrics().histograms(Boolean.parseBoolean(value));
               break;
            }
            case PREFIX: {
               builder.metrics().prefix(value);
               break;
            }
            case NAMES_AS_TAGS: {
               builder.metrics().namesAsTags(Boolean.parseBoolean(value));
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }

      ParseUtils.requireNoContent(reader);
   }

   private void parseJmx(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      GlobalConfigurationBuilder builder = holder.getGlobalConfigurationBuilder();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case ENABLED: {
               builder.jmx().enabled(Boolean.parseBoolean(value));
               break;
            }
            case DOMAIN: {
               builder.jmx().domain(value);
               break;
            }
            case MBEAN_SERVER_LOOKUP: {
               builder.jmx().mBeanServerLookup(Util.getInstance(value, holder.getClassLoader()));
               break;
            }
            case ALLOW_DUPLICATE_DOMAINS: {
               if (!reader.getSchema().since(11, 0)) {
                  ignoreAttribute(reader, i);
                  break;
               } else {
                  throw ParseUtils.attributeRemoved(reader, i);
               }
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }

      Properties properties = parseProperties(reader);
      builder.jmx().withProperties(properties);
   }

   private void parseModules(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder)
         throws XMLStreamException {
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         reader.handleAny(holder);
      }
   }

   private void parseTransport(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      if (holder.getGlobalConfigurationBuilder().transport().getTransport() == null) {
         holder.getGlobalConfigurationBuilder().transport().defaultTransport();
      }
      TransportConfigurationBuilder transport = holder.getGlobalConfigurationBuilder().transport();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case STACK: {
               // Make sure we have the default stacks added
               addJGroupsDefaultStacksIfNeeded(reader, holder);
               JGroupsChannelConfigurator jGroupsStack = holder.getJGroupsStack(value);
               if (jGroupsStack == null) {
                  throw CONFIG.missingJGroupsStack(value);
               }
               Properties p = new Properties();
               p.put(JGroupsTransport.CHANNEL_CONFIGURATOR, jGroupsStack);
               p.put("stack", value);
               transport.withProperties(p);
               break;
            }
            case CLUSTER: {
               transport.clusterName(value);
               break;
            }
            case EXECUTOR: {
               if (reader.getSchema().since(11, 0)) {
                  throw ParseUtils.attributeRemoved(reader, i);
               } else {
                  CONFIG.ignoredAttribute("transport executor", "11.0", attribute.getLocalName(), reader.getLocation().getLineNumber());
               }
               break;
            }
            case TOTAL_ORDER_EXECUTOR: {
               if (reader.getSchema().since(9, 0)) {
                  throw ParseUtils.attributeRemoved(reader, i);
               } else {
                  CONFIG.ignoredAttribute("total order executor", "9.0", attribute.getLocalName(), reader.getLocation().getLineNumber());
               }
               break;
            }
            case REMOTE_COMMAND_EXECUTOR: {
               if (reader.getSchema().since(11, 0)) {
                  throw ParseUtils.attributeRemoved(reader, i);
               } else {
                  CONFIG.ignoredAttribute("remote command executor", "11.0", attribute.getLocalName(), reader.getLocation().getLineNumber());
               }
               break;
            }
            case LOCK_TIMEOUT: {
               transport.distributedSyncTimeout(Long.parseLong(value));
               break;
            }
            case NODE_NAME: {
               transport.nodeName(value);
               holder.getGlobalConfigurationBuilder().threads().nodeName(value);
               break;
            }
            case LOCKING:
               break;
            case MACHINE_ID: {
               transport.machineId(value);
               break;
            }
            case RACK_ID: {
               transport.rackId(value);
               break;
            }
            case SITE: {
               transport.siteId(value);
               break;
            }
            case INITIAL_CLUSTER_SIZE: {
               if (reader.getSchema().since(8, 2)) {
                  transport.initialClusterSize(Integer.parseInt(value));
               } else {
                  throw ParseUtils.unexpectedAttribute(reader, i);
               }
               break;
            }
            case INITIAL_CLUSTER_TIMEOUT: {
               if (reader.getSchema().since(8, 2)) {
                  transport.initialClusterTimeout(Long.parseLong(value), TimeUnit.MILLISECONDS);
               } else {
                  throw ParseUtils.unexpectedAttribute(reader, i);
               }
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
      Properties properties = parseProperties(reader);
      for (Map.Entry<Object, Object> propertyEntry : properties.entrySet()) {
        transport.addProperty((String) propertyEntry.getKey(), (String) propertyEntry.getValue());
      }
   }

   private void parseGlobalState(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      ParseUtils.requireNoAttributes(reader);
      GlobalStateConfigurationBuilder builder = holder.getGlobalConfigurationBuilder().globalState().enable();
      ConfigurationStorage storage = null;
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case PERSISTENT_LOCATION: {
               parseGlobalStatePath(reader, builder::persistentLocation);
               break;
            }
            case SHARED_PERSISTENT_LOCATION: {
               parseGlobalStatePath(reader, builder::sharedPersistentLocation);
               break;
            }
            case TEMPORARY_LOCATION: {
               parseGlobalStatePath(reader, builder::temporaryLocation);
               break;
            }
            case IMMUTABLE_CONFIGURATION_STORAGE: {
               if (storage != null) {
                  throw ParseUtils.unexpectedElement(reader);
               }
               storage = ConfigurationStorage.IMMUTABLE;
               break;
            }
            case VOLATILE_CONFIGURATION_STORAGE: {
               if (storage != null) {
                  throw ParseUtils.unexpectedElement(reader);
               }
               ParseUtils.requireNoAttributes(reader);
               ParseUtils.requireNoContent(reader);
               storage = ConfigurationStorage.VOLATILE;
               break;
            }
            case OVERLAY_CONFIGURATION_STORAGE: {
               if (storage != null) {
                  throw ParseUtils.unexpectedElement(reader);
               }
               ParseUtils.requireNoAttributes(reader);
               ParseUtils.requireNoContent(reader);
               storage = ConfigurationStorage.OVERLAY;
               break;
            }
            case MANAGED_CONFIGURATION_STORAGE: {
               if (storage != null) {
                  throw ParseUtils.unexpectedElement(reader);
               } else {
                  throw CONFIG.managerConfigurationStorageUnavailable();
               }
            }
            case CUSTOM_CONFIGURATION_STORAGE: {
               if (storage != null) {
                  throw ParseUtils.unexpectedElement(reader);
               }
               storage = ConfigurationStorage.CUSTOM;
               builder.configurationStorageSupplier(parseCustomConfigurationStorage(reader, holder));
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
      if (storage != null) {
         builder.configurationStorage(storage);
      }
   }

   private void parseGlobalStatePath(XMLExtendedStreamReader reader, BiConsumer<String, String> pathItems) throws XMLStreamException {
      String path = ParseUtils.requireAttributes(reader, Attribute.PATH.getLocalName())[0];
      String relativeTo = null;
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case RELATIVE_TO: {
               relativeTo = ParseUtils.requireAttributeProperty(reader, i);
               break;
            }
            case PATH: {
               // Handled above
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
      ParseUtils.requireNoContent(reader);
      pathItems.accept(path, relativeTo);
   }

   private Supplier<? extends LocalConfigurationStorage> parseCustomConfigurationStorage(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      String storageClass = ParseUtils.requireSingleAttribute(reader, Attribute.CLASS.getLocalName());
      ParseUtils.requireNoContent(reader);
      return Util.getInstanceSupplier(storageClass, holder.getClassLoader());
   }

   private ThreadPoolConfiguration createThreadPoolConfiguration(String threadPoolName, String componentName, ConfigurationBuilderHolder holder) {
      ThreadsConfigurationBuilder threads = holder.getGlobalConfigurationBuilder().threads();
      ThreadPoolBuilderAdapter threadPool = threads.getThreadPool(threadPoolName);

      if (threadPool == null)
         throw CONFIG.undefinedThreadPoolName(threadPoolName);

      ThreadPoolConfiguration threadPoolConfiguration = threadPool.asThreadPoolConfigurationBuilder();
      boolean isNonBlocking = threadPoolConfiguration.threadPoolFactory().createsNonBlockingThreads();
      if (NON_BLOCKING_EXECUTOR.equals(componentName) && !isNonBlocking) {
         throw CONFIG.threadPoolFactoryIsBlocking(threadPoolName, componentName);
      }
      DefaultThreadFactory threadFactory = threadPoolConfiguration.threadFactory();
      if (threadFactory != null) {
         threadFactory.setComponent(shortened(componentName));
      }
      return threadPoolConfiguration;
   }

   /**
    * This method is public static so that it can be reused by custom cache store/loader configuration parsers
    */
   public static void parseStoreAttribute(XMLExtendedStreamReader reader, int index, AbstractStoreConfigurationBuilder<?, ?> storeBuilder) throws XMLStreamException {
      // Stores before 10.0 were non segmented by default
      if (reader.getSchema().getMajor() < 10) {
         storeBuilder.segmented(false);
      }
      String value = reader.getAttributeValue(index);
      Attribute attribute = Attribute.forName(reader.getAttributeLocalName(index));
      switch (attribute) {
         case SHARED: {
            storeBuilder.shared(Boolean.parseBoolean(value));
            break;
         }
         case READ_ONLY: {
            storeBuilder.ignoreModifications(Boolean.parseBoolean(value));
            break;
         }
         case PRELOAD: {
            storeBuilder.preload(Boolean.parseBoolean(value));
            break;
         }
         case FETCH_STATE: {
            storeBuilder.fetchPersistentState(Boolean.parseBoolean(value));
            break;
         }
         case PURGE: {
            storeBuilder.purgeOnStartup(Boolean.parseBoolean(value));
            break;
         }
         case SINGLETON: {
            if (reader.getSchema().since(10, 0)) {
               throw ParseUtils.attributeRemoved(reader, index);
            } else {
               ignoreAttribute(reader, index);
            }
            break;
         }
         case TRANSACTIONAL: {
            storeBuilder.transactional(Boolean.parseBoolean(value));
            break;
         }
         case MAX_BATCH_SIZE: {
            storeBuilder.maxBatchSize(Integer.parseInt(value));
            break;
         }
         case SEGMENTED: {
            storeBuilder.segmented(Boolean.parseBoolean(value));
            break;
         }
         default: {
            throw ParseUtils.unexpectedAttribute(reader, index);
         }
      }
   }

   public static void parseStoreElement(XMLExtendedStreamReader reader, StoreConfigurationBuilder<?, ?> storeBuilder) throws XMLStreamException {
      Element element = Element.forName(reader.getLocalName());
      switch (element) {
         case WRITE_BEHIND: {
            parseStoreWriteBehind(reader, storeBuilder.async().enable());
            break;
         }
         case PROPERTY: {
            parseStoreProperty(reader, storeBuilder);
            break;
         }
         default: {
            throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   public static void parseStoreWriteBehind(XMLExtendedStreamReader reader, AsyncStoreConfigurationBuilder<?> storeBuilder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case FLUSH_LOCK_TIMEOUT:
            case SHUTDOWN_TIMEOUT: {
               if (reader.getSchema().since(9, 0)) {
                  throw ParseUtils.attributeRemoved(reader, i);
               } else {
                  ignoreAttribute(reader, i);
               }
               break;
            }
            case MODIFICATION_QUEUE_SIZE: {
               storeBuilder.modificationQueueSize(Integer.parseInt(value));
               break;
            }
            case FAIL_SILENTLY:
               storeBuilder.failSilently(Boolean.parseBoolean(value));
               break;
            case THREAD_POOL_SIZE: {
               if (reader.getSchema().since(11, 0)) {
                  throw ParseUtils.attributeRemoved(reader, i);
               } else {
                  ignoreAttribute(reader, i);
               }
               break;
            }
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   public static void parseStoreProperty(XMLExtendedStreamReader reader, StoreConfigurationBuilder<?, ?> storeBuilder) throws XMLStreamException {
      String property = ParseUtils.requireSingleAttribute(reader, Attribute.NAME.getLocalName());
      String value = reader.getElementText();
      storeBuilder.addProperty(property, value);
   }

   private static void parseProperty(XMLExtendedStreamReader reader, Properties properties) throws XMLStreamException {
      int attributes = reader.getAttributeCount();
      ParseUtils.requireAttributes(reader, Attribute.NAME.getLocalName());
      String key = null;
      String propertyValue;
      for (int i = 0; i < attributes; i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case NAME: {
               key = value;
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
      propertyValue = reader.getElementText();
      properties.setProperty(key, propertyValue);
   }

   public static Properties parseProperties(final XMLExtendedStreamReader reader) throws XMLStreamException {
      Properties properties = new Properties();
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case PROPERTY: {
               parseProperty(reader, properties);
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
      return properties;
   }

   private void addJGroupsDefaultStacksIfNeeded(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder) {
      if (holder.getJGroupsStack(BuiltinJGroupsChannelConfigurator.TCP_STACK_NAME) == null) {
         holder.addJGroupsStack(BuiltinJGroupsChannelConfigurator.TCP(reader.getProperties()));
         holder.addJGroupsStack(BuiltinJGroupsChannelConfigurator.UDP(reader.getProperties()));
         holder.addJGroupsStack(BuiltinJGroupsChannelConfigurator.KUBERNETES(reader.getProperties()));
         holder.addJGroupsStack(BuiltinJGroupsChannelConfigurator.EC2(reader.getProperties()));
         holder.addJGroupsStack(BuiltinJGroupsChannelConfigurator.GOOGLE(reader.getProperties()));
         holder.addJGroupsStack(BuiltinJGroupsChannelConfigurator.AZURE(reader.getProperties()));
      }
   }

   @Override
   public Namespace[] getNamespaces() {
      return ParseUtils.getNamespaceAnnotations(getClass());
   }

}
