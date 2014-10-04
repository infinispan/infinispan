package org.infinispan.lucene.impl;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.infinispan.Cache;
import org.infinispan.atomic.Delta;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.commands.write.AbstractDataWriteCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.lucene.FileListCacheKey;
import org.infinispan.lucene.directory.DirectoryBuilder;
import org.infinispan.lucene.testutils.LuceneSettings;
import org.infinispan.remoting.InboundInvocationHandler;
import org.infinispan.remoting.InboundInvocationHandlerImpl;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.CommandAwareRpcDispatcher;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.util.concurrent.ConcurrentHashSet;
import org.jgroups.blocks.Response;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Set;

import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.replaceComponent;
import static org.infinispan.test.TestingUtil.replaceField;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

/**
 * Test to verify the FileListCacheValue is being replicated incrementally
 *
 * @author gustavonalle
 * @since 7.0
 */
@Test(groups = "functional", testName = "lucene.DeltaReplicationTest")
public class DeltaReplicationTest extends MultipleCacheManagersTest {

   private static final String INDEX_NAME = "index";

   @Test
   public void testDeltasAreSent() throws Exception {
      Cache<Object, Object> cache0 = cache(0);
      Cache<Object, Object> cache1 = cache(1);
      Directory dir = DirectoryBuilder.newDirectoryInstance(cache0, cache0, cache0, INDEX_NAME).create();

      InboundInvocationHandlerDecorator handler = new InboundInvocationHandlerDecorator();
      replaceOn(cache0, handler);
      replaceOn(cache1, handler);

      writeSingleDocument(dir);

      assertFileListMatch(cache0, cache1, INDEX_NAME);
      assertOnlyDeltasWereSent(handler, FileListCacheKey.class);
   }

   private void assertOnlyDeltasWereSent(InboundInvocationHandlerDecorator handler, Class<?> clazz) {
      Set<AbstractDataWriteCommand> writeCommands = handler.writeCommands;
      for (AbstractDataWriteCommand command : writeCommands) {
         if (command instanceof PutKeyValueCommand) {
            PutKeyValueCommand putKeyValueCommand = (PutKeyValueCommand) command;
            if (putKeyValueCommand.getKey().getClass().equals(clazz)) {
               Object value = putKeyValueCommand.getValue();
               assertTrue(value instanceof Delta);
            }
         }
      }
   }

   private void assertFileListMatch(Cache cache, Cache another, String index) {
      assertEquals(extract(cache, index), extract(another, index));
   }

   private FileListCacheValue extract(Cache cache, String index) {
      DataContainer dataContainer = extractComponent(cache, DataContainer.class);
      InternalCacheEntry ice = dataContainer.get(new FileListCacheKey(index));
      return (FileListCacheValue) ice.getValue();
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder c = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      createCluster(c, 2);
      waitForClusterToForm();
   }

   private void replaceOn(Cache cache, InboundInvocationHandler replacement) {
      replaceComponent(cache.getCacheManager(), InboundInvocationHandler.class, replacement, true);
      JGroupsTransport t = (JGroupsTransport) extractComponent(cache, Transport.class);
      CommandAwareRpcDispatcher card = t.getCommandAwareRpcDispatcher();
      replaceField(replacement, "inboundInvocationHandler", card, CommandAwareRpcDispatcher.class);
   }

   private void writeSingleDocument(Directory dir) throws IOException {
      IndexWriter indexWriter = LuceneSettings.openWriter(dir, 10);
      Document document = new Document();
      document.add(new StringField("field", "value", Field.Store.YES));
      indexWriter.addDocument(document);
      indexWriter.close();
   }

   class InboundInvocationHandlerDecorator extends InboundInvocationHandlerImpl {
      final Set<AbstractDataWriteCommand> writeCommands = new ConcurrentHashSet<>();

      @Override
      public void handle(CacheRpcCommand cmd, Address origin, Response response, boolean preserveOrder) throws Throwable {
         if (cmd instanceof SingleRpcCommand) {
            SingleRpcCommand singleRpcCommand = (SingleRpcCommand) cmd;
            ReplicableCommand command = singleRpcCommand.getCommand();
            if (command instanceof AbstractDataWriteCommand) {
               writeCommands.add((AbstractDataWriteCommand) command);
            }
         }
         super.handle(cmd, origin, response, preserveOrder);
      }
   }

}
