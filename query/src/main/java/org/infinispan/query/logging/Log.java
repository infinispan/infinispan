package org.infinispan.query.logging;

import java.io.IOException;
import java.util.List;

import org.hibernate.search.backend.LuceneWork;
import org.infinispan.commons.CacheException;
import org.infinispan.remoting.transport.Address;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

import static org.jboss.logging.Logger.Level.*;

/**
 * Log abstraction for the query module. For this module, message ids
 * ranging from 14001 to 15000 inclusively have been reserved.
 *
 * @author Galder Zamarreño
 * @author Sanne Grinovero
 * @since 5.0
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends org.infinispan.util.logging.Log {

   @LogMessage(level = ERROR)
   @Message(value = "Could not locate key class %s", id = 14001)
   void keyClassNotFound(String keyClassName, @Cause Exception e);

   @LogMessage(level = ERROR)
   @Message(value = "Cannot instantiate an instance of Transformer class %s", id = 14002)
   void couldNotInstantiaterTransformerClass(Class<?> transformer, @Cause Exception e);

   @LogMessage(level = INFO)
   @Message(value = "Registering Query interceptor", id = 14003)
   void registeringQueryInterceptor();

   @LogMessage(level = DEBUG)
   @Message(value = "Custom commands backend initialized backing index %s", id = 14004)
   void commandsBackendInitialized(String indexName);

   @LogMessage(level = DEBUG)
   @Message(value = "Sent list of LuceneWork %s to node %s", id = 14005)
   void workListRemotedTo(Object workList, Address primaryNodeAddress);

   @LogMessage(level = DEBUG)
   @Message(value = "Apply list of LuceneWork %s delegating to local indexing engine", id = 14006)
   void applyingChangeListLocally(List<LuceneWork> workList);

   @LogMessage(level = DEBUG)
   @Message(value = "Going to ship list of LuceneWork %s to a remote master indexer", id = 14007)
   void applyingChangeListRemotely(List<LuceneWork> workList);

   @LogMessage(level = WARN)
   @Message(value = "Index named '%1$s' is ignoring configuration option 'directory_provider' set '%2$s':" +
   		" overriden to use the Infinispan Directory", id = 14008)
   void ignoreDirectoryProviderProperty(String indexName, String directoryOption);

   @LogMessage(level = WARN)
   @Message(value = "Indexed type '%1$s' is using a default Transformer. This is slow! Register a custom implementation using @Transformable", id = 14009)
   void typeIsUsingDefaultTransformer(Class<?> keyClass);

   @Message(value = "An IOException happened where none where expected", id = 14010)
   CacheException unexpectedIOException(@Cause IOException e);
}
