package org.infinispan.configuration.parsing;

import static org.infinispan.configuration.parsing.ParseUtils.ignoreAttribute;
import static org.infinispan.configuration.parsing.ParseUtils.ignoreElement;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import org.infinispan.commons.configuration.io.ConfigurationReader;
import org.infinispan.commons.configuration.io.ConfigurationResourceResolver;
import org.infinispan.commons.configuration.io.NamingStrategy;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.AsyncStoreConfigurationBuilder;
import org.infinispan.configuration.cache.StoreConfigurationBuilder;
import org.infinispan.configuration.global.AllowListConfigurationBuilder;
import org.infinispan.configuration.global.GlobalAuthorizationConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.global.GlobalRoleConfigurationBuilder;
import org.infinispan.configuration.global.GlobalSecurityConfigurationBuilder;
import org.infinispan.configuration.global.GlobalStateConfigurationBuilder;
import org.infinispan.configuration.global.SerializationConfigurationBuilder;
import org.infinispan.configuration.global.ShutdownHookBehavior;
import org.infinispan.configuration.global.ThreadPoolBuilderAdapter;
import org.infinispan.configuration.global.ThreadPoolConfiguration;
import org.infinispan.configuration.global.ThreadsConfigurationBuilder;
import org.infinispan.configuration.global.TracingExporterProtocol;
import org.infinispan.configuration.global.TransportConfigurationBuilder;
import org.infinispan.factories.threads.DefaultThreadFactory;
import org.infinispan.globalstate.ConfigurationStorage;
import org.infinispan.globalstate.LocalConfigurationStorage;
import org.infinispan.protostream.config.Configuration;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.BuiltinJGroupsChannelConfigurator;
import org.infinispan.remoting.transport.jgroups.EmbeddedJGroupsChannelConfigurator;
import org.infinispan.remoting.transport.jgroups.FileJGroupsChannelConfigurator;
import org.infinispan.security.PrincipalRoleMapper;
import org.infinispan.security.RolePermissionMapper;
import org.infinispan.security.mappers.CaseNameRewriter;
import org.infinispan.security.mappers.ClusterPermissionMapper;
import org.infinispan.security.mappers.ClusterRoleMapper;
import org.infinispan.security.mappers.CommonNameRoleMapper;
import org.infinispan.security.mappers.IdentityRoleMapper;
import org.infinispan.security.mappers.RegexNameRewriter;
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
   public void readElement(final ConfigurationReader reader, final ConfigurationBuilderHolder holder) {
      addJGroupsDefaultStacksIfNeeded(reader, holder);
      while (reader.inTag("infinispan")) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case CACHE_CONTAINER: {
               parseContainer(reader, holder);
               break;
            }
            case JGROUPS: {
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

   private void parseSerialization(final ConfigurationReader reader, final ConfigurationBuilderHolder holder) {
      GlobalConfigurationBuilder builder = holder.getGlobalConfigurationBuilder();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));

         switch (attribute) {
            case MARSHALLER: {
               builder.serialization().marshaller(Util.getInstance(value, holder.getClassLoader()));
               break;
            }
            case SCHEMA_COMPATIBILITY: {
               builder.serialization().schemaCompatibilityValidation(ParseUtils.parseEnum(reader, i, Configuration.SchemaValidation.class, value));
               break;
            }
            case VERSION: {
               ParseUtils.removedSince(reader, 11, 0);
               ignoreAttribute(reader, i);
               break;
            }
            case CONTEXT_INITIALIZERS: {
               for (String klass : reader.getListAttributeValue(i)) {
                  builder.serialization().addContextInitializer(Util.getInstance(klass, holder.getClassLoader()));
               }
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }


      while (reader.hasNext()) {
         reader.nextElement();
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case SERIALIZATION: {
               reader.require(ConfigurationReader.ElementType.END_ELEMENT);
               return;
            }
            case ADVANCED_EXTERNALIZERS:
               // Empty elements for YAML/JSON
               break;
            case SERIALIZATION_CONTEXT_INITIALIZERS: {
               if (reader.getAttributeCount() > 0) {
                  parseSerializationContextInitializer(reader, holder.getClassLoader(), builder.serialization());
               }
               break;
            }
            case ADVANCED_EXTERNALIZER: {
               ParseUtils.removedSince(reader, 16, 0);
               ignoreElement(reader, element);
               break;
            }
            case SERIALIZATION_CONTEXT_INITIALIZER: {
               parseSerializationContextInitializer(reader, holder.getClassLoader(), builder.serialization());
               break;
            }
            case WHITE_LIST:
               ParseUtils.removedSince(reader, 12, 0);
               CONFIG.configDeprecatedUseOther(Element.WHITE_LIST, Element.ALLOW_LIST, reader.getLocation());
               parseAllowList(reader, builder.serialization().allowList(), Element.WHITE_LIST);
               break;
            case ALLOW_LIST: {
               parseAllowList(reader, builder.serialization().allowList(), Element.ALLOW_LIST);
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
   }

   private void parseSerializationContextInitializer(final ConfigurationReader reader, final ClassLoader classLoader,
                                                     final SerializationConfigurationBuilder builder) {
      int attributes = reader.getAttributeCount();
      ParseUtils.requireAttributes(reader, Attribute.CLASS.getLocalName());
      for (int i = 0; i < attributes; i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
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

   private void parseAllowList(final ConfigurationReader reader, final AllowListConfigurationBuilder builder, Element outerElement) {
      for(int i = 0; i < reader.getAttributeCount(); i++) { // JSON/YAML
         String[] values = reader.getListAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case CLASS:
               builder.addClasses(values);
               break;
            case REGEX:
               builder.addRegexps(values);
               break;
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
      while (reader.inTag(outerElement)) { // XML
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

   private void parseThreads(ConfigurationReader reader, ConfigurationBuilderHolder holder) {
      while (reader.hasNext()) {
         reader.nextElement();
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case THREADS: {
               reader.require(ConfigurationReader.ElementType.END_ELEMENT);
               return;
            }
            case THREAD_FACTORIES: {
               while (reader.inTag(Element.THREAD_FACTORIES)) {
                  parseThreadFactory(reader, holder);
               }
               break;
            }
            case THREAD_POOLS: {
               while (reader.inTag(Element.THREAD_POOLS)) {
                  parseThreadPools(reader, holder);
               }
               break;
            }
            case THREAD_FACTORY: {
               parseThreadFactory(reader, holder);
               break;
            }
            default: {
               parseThreadPools(reader, holder);
            }
         }
      }
   }

   private void parseThreadPools(ConfigurationReader reader, ConfigurationBuilderHolder holder) {
      Map.Entry<String, String> threadPool = reader.getMapItem(Attribute.NAME);
      Element element = Element.forName(threadPool.getValue());
      switch (element) {
         case CACHED_THREAD_POOL: {
            parseCachedThreadPool(reader, holder, threadPool.getKey());
            break;
         }
         case SCHEDULED_THREAD_POOL: {
            parseScheduledThreadPool(reader, holder, threadPool.getKey());
            break;
         }
         case BLOCKING_BOUNDED_QUEUE_THREAD_POOL: {
            parseBoundedQueueThreadPool(reader, holder, threadPool.getKey(), false);
            break;
         }
         case NON_BLOCKING_BOUNDED_QUEUE_THREAD_POOL: {
            parseBoundedQueueThreadPool(reader, holder, threadPool.getKey(), true);
            break;
         }
         default: {
            throw ParseUtils.unexpectedElement(reader);
         }
      }
      reader.endMapItem();
   }

   public void parseBoundedQueueThreadPool(ConfigurationReader reader, ConfigurationBuilderHolder holder,
                                           String name, boolean isNonBlocking) {
      ThreadsConfigurationBuilder threadsBuilder = holder.getGlobalConfigurationBuilder().threads();
      String threadFactoryName = null;
      int maxThreads = 0;
      int coreThreads = 0;
      int queueLength = 0;
      long keepAlive = 0;

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));

         switch (attribute) {
            case NAME: {
               // Already seen
               break;
            }
            case THREAD_FACTORY: {
               threadFactoryName = value;
               break;
            }
            case CORE_THREADS: {
               coreThreads = ParseUtils.parseInt(reader, i, value);
               break;
            }
            case MAX_THREADS: {
               maxThreads = ParseUtils.parseInt(reader, i, value);
               break;
            }
            case QUEUE_LENGTH: {
               queueLength = ParseUtils.parseInt(reader, i, value);
               break;
            }
            case KEEP_ALIVE_TIME: {
               keepAlive = ParseUtils.parseLong(reader, i, value);
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

   private void parseScheduledThreadPool(ConfigurationReader reader, ConfigurationBuilderHolder holder, String name) {
      ThreadsConfigurationBuilder threadsBuilder = holder.getGlobalConfigurationBuilder().threads();
      String threadFactoryName = null;
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case NAME: {
               // Already seen
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

   private void parseCachedThreadPool(ConfigurationReader reader, ConfigurationBuilderHolder holder, String name) {
      ThreadsConfigurationBuilder threadsBuilder = holder.getGlobalConfigurationBuilder().threads();
      String threadFactoryName = null;
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));

         switch (attribute) {
            case NAME: {
               // Ignore
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

   private void parseThreadFactory(ConfigurationReader reader, ConfigurationBuilderHolder holder) {
      Map.Entry<String, String> threadFactory = reader.getMapItem(Attribute.NAME);
      String threadGroupName = null;
      String threadNamePattern = null;
      int priority = 1; // minimum priority

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));

         switch (attribute) {
            case NAME: {
               // Already seen
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
               priority = ParseUtils.parseInt(reader, i, value);
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }

      holder.getGlobalConfigurationBuilder().threads().addThreadFactory(threadFactory.getKey()).groupName(threadGroupName).priority(priority).threadNamePattern(threadNamePattern);
      ParseUtils.requireNoContent(reader);
      reader.endMapItem();
   }

   private void parseJGroups(ConfigurationReader reader, ConfigurationBuilderHolder holder) {
      Transport transport = null;
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         if (!ParseUtils.isNoNamespaceAttribute(reader, i))
            continue;
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));

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

      while (reader.inTag(Element.JGROUPS)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case STACK_FILE:
               parseStackFile(reader, holder, ParseUtils.requireAttributes(reader, Attribute.NAME)[0]);
               break;
            case STACKS:
               parseJGroupsStacks(reader, holder);
               break;
            case STACK:
               parseJGroupsStack(reader, holder, ParseUtils.requireAttributes(reader, Attribute.NAME)[0]);
               break;
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
   }

   private void parseJGroupsStacks(ConfigurationReader reader, ConfigurationBuilderHolder holder) {
      while (reader.inTag(Element.STACKS)) {
         Map.Entry<String, String> mapElement = reader.getMapItem(Attribute.NAME);
         Element type = Element.forName(mapElement.getValue());
         switch (type) {
            case STACK:
               parseJGroupsStack(reader, holder, mapElement.getKey());
               break;
            case STACK_FILE:
               parseStackFile(reader, holder, mapElement.getKey());
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }

         reader.endMapItem();
      }
   }

   private void addJGroupsStackFile(ConfigurationBuilderHolder holder, String name, String path, Properties properties, ConfigurationResourceResolver resourceResolver) {
      URL url = FileLookupFactory.newInstance().lookupFileLocation(path, holder.getClassLoader());
      try (InputStream xml = (url != null ? url : resourceResolver.resolveResource(path)).openStream()) {
         holder.addJGroupsStack(new FileJGroupsChannelConfigurator(name, path, xml, properties));
      } catch (FileNotFoundException e) {
         throw CONFIG.jgroupsConfigurationNotFound(path);
      } catch (IOException e) {
         throw CONFIG.unableToAddJGroupsStack(name, e);
      }
   }

   private void parseJGroupsStack(ConfigurationReader reader, ConfigurationBuilderHolder holder, String stackName) {
      String extend = null;
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
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

      List<ProtocolConfiguration> stack = new ArrayList<>();
      EmbeddedJGroupsChannelConfigurator.RemoteSites remoteSites = null;
      while (reader.inTag()) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case REMOTE_SITES:
               remoteSites = parseJGroupsRelay(reader, holder, stackName);
               break;
            default:
               // It should be an actual JGroups protocol
               String protocolName = reader.getLocalName(NamingStrategy.IDENTITY);
               Map<String, String> protocolAttributes = new HashMap<>();
               for (int i = 0; i < reader.getAttributeCount(); i++) {
                  protocolAttributes.put(reader.getAttributeName(i, NamingStrategy.SNAKE_CASE), reader.getAttributeValue(i));
               }
               ParseUtils.requireNoContent(reader);
               stack.add(new ProtocolConfiguration(protocolName, protocolAttributes));
               break;
         }
      }
      EmbeddedJGroupsChannelConfigurator stackConfigurator = new EmbeddedJGroupsChannelConfigurator(stackName, stack, remoteSites);
      holder.addJGroupsStack(stackConfigurator, extend);
   }

   private EmbeddedJGroupsChannelConfigurator.RemoteSites parseJGroupsRelay(ConfigurationReader reader, ConfigurationBuilderHolder holder, String stackName) {
      String defaultStack = ParseUtils.requireAttributes(reader, Attribute.DEFAULT_STACK)[0];
      String defaultCluster = "xsite";
      if (!holder.hasJGroupsStack(defaultStack)) {
         throw CONFIG.missingJGroupsStack(defaultStack);
      }
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         if (!ParseUtils.isNoNamespaceAttribute(reader, i))
            continue;
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case DEFAULT_STACK:
               // Already seen
               break;
            case CLUSTER:
               defaultCluster = value;
               break;
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
      EmbeddedJGroupsChannelConfigurator.RemoteSites remoteSites = new EmbeddedJGroupsChannelConfigurator.RemoteSites(defaultStack, defaultCluster);
      while (reader.inTag()) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case REMOTE_SITE:
               if (reader.getAttributeCount() > 0) {
                  String remoteSite = ParseUtils.requireAttributes(reader, Attribute.NAME)[0];
                  String cluster = defaultCluster;
                  String stack = defaultStack;
                  for (int i = 0; i < reader.getAttributeCount(); i++) {
                     Attribute attribute = Attribute.forName(reader.getAttributeName(i));
                     switch (attribute) {
                        case NAME:
                           break;
                        case STACK:
                           stack = reader.getAttributeValue(i);
                           break;
                        case CLUSTER:
                           cluster = reader.getAttributeValue(i);
                           break;
                        default:
                           throw ParseUtils.unexpectedAttribute(reader, i);
                     }
                  }
                  ParseUtils.requireNoContent(reader);
                  remoteSites.addRemoteSite(stackName, remoteSite, cluster, stack);
               }
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
      return remoteSites;
   }

   private void parseStackFile(ConfigurationReader reader, ConfigurationBuilderHolder holder, String name) {
      String path = ParseUtils.requireAttributes(reader, Attribute.PATH)[0];
      ParseUtils.requireNoContent(reader);

      addJGroupsStackFile(holder, name, path, reader.getProperties(), reader.getResourceResolver());
   }

   private void parseContainer(ConfigurationReader reader, ConfigurationBuilderHolder holder) {
      holder.pushScope(ParserScope.CACHE_CONTAINER);
      GlobalConfigurationBuilder builder = holder.getGlobalConfigurationBuilder();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         if (!ParseUtils.isNoNamespaceAttribute(reader, i))
            continue;
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
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
            case PERSISTENCE_EXECUTOR: {
               ParseUtils.removedSince(reader, 11, 0);
               ignoreAttribute(reader, i);
               break;
            }
            case LISTENER_EXECUTOR: {
               builder.listenerThreadPoolName(value);
               builder.listenerThreadPool().read(createThreadPoolConfiguration(value, ASYNC_NOTIFICATION_EXECUTOR, holder), holder.getCombine());
               break;
            }
            case EVICTION_EXECUTOR:
               ParseUtils.removedSince(reader, 11, 0);
               CONFIG.evictionExecutorDeprecated();

               // fallthrough
            case EXPIRATION_EXECUTOR: {
               builder.expirationThreadPoolName(value);
               builder.expirationThreadPool().read(createThreadPoolConfiguration(value, EXPIRATION_SCHEDULED_EXECUTOR, holder), holder.getCombine());
               break;
            }
            case NON_BLOCKING_EXECUTOR: {
               builder.nonBlockingThreadPoolName(value);
               builder.nonBlockingThreadPool().read(createThreadPoolConfiguration(value, NON_BLOCKING_EXECUTOR, holder), holder.getCombine());
               break;
            }
            case BLOCKING_EXECUTOR: {
               builder.blockingThreadPoolName(value);
               builder.blockingThreadPool().read(createThreadPoolConfiguration(value, BLOCKING_EXECUTOR, holder), holder.getCombine());
               break;
            }
            case STATISTICS: {
               boolean statistics = ParseUtils.parseBoolean(reader, i, value);
               builder.cacheContainer().statistics(statistics);
               if (!reader.getSchema().since(10, 1)) {
                  builder.jmx().enabled(statistics);
               }
               break;
            }
            case SHUTDOWN_HOOK: {
               builder.shutdown().hookBehavior(ParseUtils.parseEnum(reader, i, ShutdownHookBehavior.class, value));
               break;
            }
            case ZERO_CAPACITY_NODE: {
               builder.zeroCapacityNode(ParseUtils.parseBoolean(reader, i, value));
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }

      while (reader.inTag(Element.CACHE_CONTAINER)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case CACHES: {
               // Wrapper element for YAML/JSON
               parseCaches(reader, holder);
               break;
            }
            case TRANSPORT: {
               parseTransport(reader, holder);
               break;
            }
            case LOCAL_CACHE: {
               parseLocalCache(reader, holder, ParseUtils.requireAttributes(reader, Attribute.NAME)[0], false);
               break;
            }
            case LOCAL_CACHE_CONFIGURATION: {
               parseLocalCache(reader, holder, ParseUtils.requireAttributes(reader, Attribute.NAME)[0], true);
               break;
            }
            case INVALIDATION_CACHE: {
               parseInvalidationCache(reader, holder, ParseUtils.requireAttributes(reader, Attribute.NAME)[0], false);
               break;
            }
            case INVALIDATION_CACHE_CONFIGURATION: {
               parseInvalidationCache(reader, holder, ParseUtils.requireAttributes(reader, Attribute.NAME)[0], true);
               break;
            }
            case REPLICATED_CACHE: {
               parseReplicatedCache(reader, holder, ParseUtils.requireAttributes(reader, Attribute.NAME)[0], false);
               break;
            }
            case REPLICATED_CACHE_CONFIGURATION: {
               parseReplicatedCache(reader, holder, ParseUtils.requireAttributes(reader, Attribute.NAME)[0], true);
               break;
            }
            case DISTRIBUTED_CACHE: {
               parseDistributedCache(reader, holder, ParseUtils.requireAttributes(reader, Attribute.NAME)[0], false);
               break;
            }
            case DISTRIBUTED_CACHE_CONFIGURATION: {
               parseDistributedCache(reader, holder, ParseUtils.requireAttributes(reader, Attribute.NAME)[0], true);
               break;
            }
            case SCATTERED_CACHE:
            case SCATTERED_CACHE_CONFIGURATION: {
               ParseUtils.removedSince(reader, 15, 0);
               parseScatteredCache(reader, element);
               break;
            }
            case SERIALIZATION: {
               parseSerialization(reader, holder);
               break;
            }
            case METRICS: {
               parseMetrics(reader, holder);
               break;
            }
            case TRACING: {
               parseTracing(reader, holder);
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
               parseGlobalState(reader, holder);
               break;
            }
            default: {
               reader.handleAny(holder);
            }
         }
      }
      holder.popScope();
   }

   private void parseCaches(ConfigurationReader reader, ConfigurationBuilderHolder holder) {
      while (reader.inTag(Element.CACHES)) {
         Map.Entry<String, String> mapElement = reader.getMapItem(Attribute.NAME);
         String name = mapElement.getKey();
         Element type = Element.forName(mapElement.getValue());
         switch (type) {
            case LOCAL_CACHE:
               parseLocalCache(reader, holder, name, false);
               break;
            case LOCAL_CACHE_CONFIGURATION:
               parseLocalCache(reader, holder, name, true);
               break;
            case INVALIDATION_CACHE:
               parseInvalidationCache(reader, holder, name, false);
               break;
            case INVALIDATION_CACHE_CONFIGURATION:
               parseInvalidationCache(reader, holder, name, true);
               break;
            case REPLICATED_CACHE:
               parseReplicatedCache(reader, holder, name, false);
               break;
            case REPLICATED_CACHE_CONFIGURATION:
               parseReplicatedCache(reader, holder, name, true);
               break;
            case DISTRIBUTED_CACHE:
               parseDistributedCache(reader, holder, name, false);
               break;
            case DISTRIBUTED_CACHE_CONFIGURATION:
               parseDistributedCache(reader, holder, name, true);
               break;
            case SCATTERED_CACHE:
            case SCATTERED_CACHE_CONFIGURATION:
               parseScatteredCache(reader, type);
               break;
            default:
               throw ParseUtils.unexpectedElement(reader, type);
         }
         reader.endMapItem();
      }
   }

   private void parseGlobalSecurity(ConfigurationReader reader, ConfigurationBuilderHolder holder) {
      GlobalSecurityConfigurationBuilder security = holder.getGlobalConfigurationBuilder().security();
      ParseUtils.parseAttributes(reader, security);
      while (reader.inTag()) {
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

   private void parseGlobalAuthorization(ConfigurationReader reader, ConfigurationBuilderHolder holder) {
      GlobalAuthorizationConfigurationBuilder builder = holder.getGlobalConfigurationBuilder().security().authorization().enable();
      Boolean groupOnlyMapping = reader.getSchema().since(15,0 ) ? null : false;
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case AUDIT_LOGGER: {
               builder.auditLogger(Util.getInstance(value, holder.getClassLoader()));
               break;
            }
            case GROUP_ONLY_MAPPING: {
               groupOnlyMapping = ParseUtils.parseBoolean(reader, i, value);
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
      PrincipalRoleMapper roleMapper = null;
      RolePermissionMapper permissionMapper = null;
      while (reader.hasNext()) {
         reader.nextElement();
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case AUTHORIZATION: {
               reader.require(ConfigurationReader.ElementType.END_ELEMENT);
               if (permissionMapper != null) {
                  builder.rolePermissionMapper(permissionMapper);
               }
               if (roleMapper != null) {
                  builder.principalRoleMapper(roleMapper);
               }
               if (groupOnlyMapping != null) {
                  builder.groupOnlyMapping(groupOnlyMapping);
               }
               return;
            }
            case CLUSTER_PERMISSION_MAPPER:
               if (permissionMapper != null) {
                  throw ParseUtils.unexpectedElement(reader);
               }
               ParseUtils.requireNoAttributes(reader);
               ParseUtils.requireNoContent(reader);
               permissionMapper = new ClusterPermissionMapper();
               break;
            case CUSTOM_PERMISSION_MAPPER:
               if (permissionMapper != null) {
                  throw ParseUtils.unexpectedElement(reader);
               }
               permissionMapper = parseCustomPermissionMapper(reader, holder);
               break;
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
               roleMapper = parseClusterRoleMapper(reader);
               break;
            case CUSTOM_ROLE_MAPPER:
               if (roleMapper != null) {
                  throw ParseUtils.unexpectedElement(reader);
               }
               roleMapper = parseCustomRoleMapper(reader, holder);
               break;
            case ROLES: {
               while (reader.inTag()) {
                  Map.Entry<String, String> item = reader.getMapItem(Attribute.NAME);
                  parseGlobalRole(reader, builder, item.getKey());
                  reader.endMapItem();
               }
               break;
            }
            case ROLE: {
               parseGlobalRole(reader, builder, null);
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
   }

   private ClusterRoleMapper parseClusterRoleMapper(ConfigurationReader reader) {
      ParseUtils.requireNoAttributes(reader);
      ClusterRoleMapper mapper = new ClusterRoleMapper();
      while (reader.inTag()) {
         if (Element.forName(reader.getLocalName()) == Element.NAME_REWRITER) {
            while (reader.inTag()) {
               switch (Element.forName(reader.getLocalName())) {
                  case CASE_PRINCIPAL_TRANSFORMER: {
                     boolean uppercase = true;
                     for (int i = 0; i < reader.getAttributeCount(); i++) {
                        ParseUtils.requireNoNamespaceAttribute(reader, i);
                        String value = reader.getAttributeValue(i);
                        Attribute attribute = Attribute.forName(reader.getAttributeName(i));
                        if (Objects.requireNonNull(attribute) == Attribute.UPPERCASE) {
                           uppercase = ParseUtils.parseBoolean(reader, i, value);
                        } else {
                           throw ParseUtils.unexpectedAttribute(reader, i);
                        }
                     }
                     mapper.nameRewriter(new CaseNameRewriter(uppercase));
                     ParseUtils.requireNoContent(reader);
                     break;
                  }
                  case COMMON_NAME_PRINCIPAL_TRANSFORMER: {
                     ParseUtils.requireNoAttributes(reader);
                     mapper.nameRewriter(new RegexNameRewriter(Pattern.compile("cn=([^,]+),.*", Pattern.CASE_INSENSITIVE), "$1", false));
                     ParseUtils.requireNoContent(reader);
                     break;
                  }
                  case REGEX_PRINCIPAL_TRANSFORMER: {
                     String[] attributes = ParseUtils.requireAttributes(reader, Attribute.PATTERN, Attribute.REPLACEMENT);
                     boolean replaceAll = false;
                     for (int i = 0; i < reader.getAttributeCount(); i++) {
                        ParseUtils.requireNoNamespaceAttribute(reader, i);
                        String value = reader.getAttributeValue(i);
                        Attribute attribute = Attribute.forName(reader.getAttributeName(i));
                        switch (attribute) {
                           case NAME:
                           case PATTERN:
                           case REPLACEMENT:
                              // Already seen
                              break;
                           case REPLACE_ALL:
                              replaceAll = ParseUtils.parseBoolean(reader, i, value);
                              break;
                           default:
                              throw ParseUtils.unexpectedAttribute(reader, i);
                        }
                     }
                     mapper.nameRewriter(new RegexNameRewriter(Pattern.compile(attributes[0]), attributes[1], replaceAll));
                     ParseUtils.requireNoContent(reader);
                     break;
                  }
                  default:
                     throw ParseUtils.unexpectedElement(reader);
               }
            }
         }
      }
      return mapper;
   }

   private PrincipalRoleMapper parseCustomRoleMapper(ConfigurationReader reader, ConfigurationBuilderHolder holder) {
      String mapperClass = ParseUtils.requireSingleAttribute(reader, Attribute.CLASS.getLocalName());
      ParseUtils.requireNoContent(reader);
      return Util.getInstance(mapperClass, holder.getClassLoader());
   }

   private RolePermissionMapper parseCustomPermissionMapper(ConfigurationReader reader, ConfigurationBuilderHolder holder) {
      String mapperClass = ParseUtils.requireSingleAttribute(reader, Attribute.CLASS.getLocalName());
      ParseUtils.requireNoContent(reader);
      return Util.getInstance(mapperClass, holder.getClassLoader());
   }

   private void parseGlobalRole(ConfigurationReader reader, GlobalAuthorizationConfigurationBuilder builder, String name) {
      if (name == null) {
         name = ParseUtils.requireAttributes(reader, Attribute.NAME.getLocalName())[0];
      }
      String description = null;
      String[] permissions = null;
      GlobalRoleConfigurationBuilder role = builder.role(name);
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case NAME: {
               // Already handled
               break;
            }
            case PERMISSIONS: {
               permissions = reader.getListAttributeValue(i);
               break;
            }
            case DESCRIPTION: {
               description = reader.getAttributeValue(i);
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
      role.description(description);
      if (permissions != null) {
         role.permission(permissions);
      } else {
         throw ParseUtils.missingRequired(reader, Collections.singleton(Attribute.PERMISSIONS));
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseMetrics(ConfigurationReader reader, ConfigurationBuilderHolder holder) {
      GlobalConfigurationBuilder builder = holder.getGlobalConfigurationBuilder();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case GAUGES: {
               builder.metrics().gauges(ParseUtils.parseBoolean(reader, i, value));
               break;
            }
            case HISTOGRAMS: {
               builder.metrics().histograms(ParseUtils.parseBoolean(reader, i, value));
               break;
            }
            case PREFIX: {
               builder.metrics().prefix(value);
               break;
            }
            case NAMES_AS_TAGS: {
               builder.metrics().namesAsTags(ParseUtils.parseBoolean(reader, i, value));
               break;
            }
            case ACCURATE_SIZE: {
               builder.metrics().accurateSize(ParseUtils.parseBoolean(reader, i, value));
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }

      ParseUtils.requireNoContent(reader);
   }

   private void parseTracing(ConfigurationReader reader, ConfigurationBuilderHolder holder) {
      GlobalConfigurationBuilder builder = holder.getGlobalConfigurationBuilder();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case COLLECTOR_ENDPOINT: {
               builder.tracing().collectorEndpoint(value);
               break;
            }
            case ENABLED: {
               builder.tracing().enabled(ParseUtils.parseBoolean(reader, i, value));
               break;
            }
            case EXPORTER_PROTOCOL: {
               builder.tracing().exporterProtocol(ParseUtils.parseEnum(reader, i, TracingExporterProtocol.class, value));
               break;
            }
            case SERVICE_NAME: {
               builder.tracing().serviceName(value);
               break;
            }
            case SECURITY: {
               boolean security = ParseUtils.parseBoolean(reader, i, value);
               builder.tracing().traceSecurity(security);
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
   }

   private void parseJmx(ConfigurationReader reader, ConfigurationBuilderHolder holder) {
      GlobalConfigurationBuilder builder = holder.getGlobalConfigurationBuilder();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case ENABLED: {
               builder.jmx().enabled(ParseUtils.parseBoolean(reader, i, value));
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

      Properties properties = parseProperties(reader, Element.JMX);
      builder.jmx().withProperties(properties);
   }

   private void parseModules(ConfigurationReader reader, ConfigurationBuilderHolder holder) {
      while (reader.inTag()) {
         reader.handleAny(holder);
      }
   }

   private void parseTransport(ConfigurationReader reader, ConfigurationBuilderHolder holder) {
      if (holder.getGlobalConfigurationBuilder().transport().getTransport() == null) {
         holder.getGlobalConfigurationBuilder().transport().defaultTransport();
      }
      TransportConfigurationBuilder transport = holder.getGlobalConfigurationBuilder().transport();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         if (ParseUtils.isNoNamespaceAttribute(reader, i)) {
            Attribute attribute = Attribute.forName(reader.getAttributeName(i));
            switch (attribute) {
               case STACK: {
                  transport.stack(value);
                  break;
               }
               case CLUSTER: {
                  transport.clusterName(value);
                  break;
               }
               case EXECUTOR:
               case REMOTE_COMMAND_EXECUTOR: {
                  ParseUtils.removedSince(reader, 11, 0);
                  break;
               }
               case LOCK_TIMEOUT: {
                  transport.distributedSyncTimeout(ParseUtils.parseLong(reader, i, value));
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
                  ParseUtils.introducedFrom(reader, 8, 2);
                  transport.initialClusterSize(ParseUtils.parseInt(reader, i, value));
                  break;
               }
               case INITIAL_CLUSTER_TIMEOUT: {
                  transport.initialClusterTimeout(ParseUtils.parseLong(reader, i, value), TimeUnit.MILLISECONDS);
                  break;
               }
               case RAFT_MEMBERS:
                  transport.raftMembers(reader.getListAttributeValue(i));
                  break;
               default: {
                  throw ParseUtils.unexpectedAttribute(reader, i);
               }
            }
         } else {
            reader.handleAttribute(holder, i);
         }
      }
      Properties properties = parseProperties(reader, Element.TRANSPORT);
      for (Map.Entry<Object, Object> propertyEntry : properties.entrySet()) {
         transport.addProperty((String) propertyEntry.getKey(), propertyEntry.getValue());
      }
   }

   private void parseGlobalState(ConfigurationReader reader, ConfigurationBuilderHolder holder) {
      GlobalStateConfigurationBuilder builder = holder.getGlobalConfigurationBuilder().globalState().enable();
      ParseUtils.parseAttributes(reader, builder);
      ConfigurationStorage storage = null;
      while (reader.inTag()) {
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

   private void parseGlobalStatePath(ConfigurationReader reader, BiConsumer<String, String> pathItems) {
      String path = ParseUtils.requireAttributes(reader, Attribute.PATH.getLocalName())[0];
      String relativeTo = null;
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
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

   private Supplier<? extends LocalConfigurationStorage> parseCustomConfigurationStorage(ConfigurationReader reader, ConfigurationBuilderHolder holder) {
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
    * @deprecated use {@link CacheParser#parseStoreAttribute(ConfigurationReader, int, AbstractStoreConfigurationBuilder)}
    */
   @Deprecated(forRemoval=true, since = "12.0")
   public static void parseStoreAttribute(ConfigurationReader reader, int index, AbstractStoreConfigurationBuilder<?, ?> storeBuilder) {
      CacheParser.parseStoreAttribute(reader, index, storeBuilder);
   }

   /**
    * @deprecated use {@link CacheParser#parseStoreElement(ConfigurationReader, StoreConfigurationBuilder)}
    */
   @Deprecated(forRemoval=true, since = "12.0")
   public static void parseStoreElement(ConfigurationReader reader, StoreConfigurationBuilder<?, ?> storeBuilder) {
      CacheParser.parseStoreElement(reader, storeBuilder);
   }

   /**
    * @deprecated use {@link CacheParser#parseStoreWriteBehind(ConfigurationReader, AsyncStoreConfigurationBuilder)}
    */
   @Deprecated(forRemoval=true, since = "12.0")
   public static void parseStoreWriteBehind(ConfigurationReader reader, AsyncStoreConfigurationBuilder<?> storeBuilder) {
      CacheParser.parseStoreWriteBehind(reader, storeBuilder);
   }

   /**
    * @deprecated use {@link CacheParser#parseStoreProperty(ConfigurationReader, StoreConfigurationBuilder)}
    */
   @Deprecated(forRemoval=true, since = "12.0")
   public static void parseStoreProperty(ConfigurationReader reader, StoreConfigurationBuilder<?, ?> storeBuilder) {
      String property = ParseUtils.requireSingleAttribute(reader, Attribute.NAME.getLocalName());
      String value = reader.getElementText();
      storeBuilder.addProperty(property, value);
   }

   private void addJGroupsDefaultStacksIfNeeded(final ConfigurationReader reader, final ConfigurationBuilderHolder holder) {
      if (!holder.hasJGroupsStack(BuiltinJGroupsChannelConfigurator.TCP_STACK_NAME)) {
         holder.addJGroupsStack(BuiltinJGroupsChannelConfigurator.TCP(reader.getProperties()));
         holder.addJGroupsStack(BuiltinJGroupsChannelConfigurator.UDP(reader.getProperties()));
         holder.addJGroupsStack(BuiltinJGroupsChannelConfigurator.KUBERNETES(reader.getProperties()));
         holder.addJGroupsStack(BuiltinJGroupsChannelConfigurator.EC2(reader.getProperties()));
         holder.addJGroupsStack(BuiltinJGroupsChannelConfigurator.GOOGLE(reader.getProperties()));
         holder.addJGroupsStack(BuiltinJGroupsChannelConfigurator.AZURE(reader.getProperties()));
         holder.addJGroupsStack(BuiltinJGroupsChannelConfigurator.TUNNEL(reader.getProperties()));
      }
   }
}
