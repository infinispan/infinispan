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
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.lucene.FileListCacheKey;
import org.infinispan.lucene.directory.DirectoryBuilder;
import org.infinispan.lucene.testutils.LuceneSettings;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.Reply;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.commons.util.concurrent.ConcurrentHashSet;
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

      InboundInvocationHandlerDecorator handler0 = replaceOn(cache0);
      InboundInvocationHandlerDecorator handler1 = replaceOn(cache1);

      writeSingleDocument(dir);

      assertFileListMatch(cache0, cache1, INDEX_NAME);
      assertOnlyDeltasWereSent(handler0, FileListCacheKey.class);
      assertOnlyDeltasWereSent(handler1, FileListCacheKey.class);
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
      InternalCacheEntry ice = dataContainer.get(new FileListCacheKey(index, -1));
      return (FileListCacheValue) ice.getValue();
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder c = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      createCluster(c, 2);
      waitForClusterToForm();
   }

   private InboundInvocationHandlerDecorator replaceOn(Cache cache) {
      InboundInvocationHandlerDecorator decorator = new InboundInvocationHandlerDecorator(
            extractComponent(cache, PerCacheInboundInvocationHandler.class));
      replaceComponent(cache, PerCacheInboundInvocationHandler.class, decorator, true);
      replaceField(decorator, "inboundInvocationHandler", cache.getAdvancedCache().getComponentRegistry(), ComponentRegistry.class);
      return decorator;
   }

   private void writeSingleDocument(Directory dir) throws IOException {
      IndexWriter indexWriter = LuceneSettings.openWriter(dir, 10);
      Document document = new Document();
      document.add(new StringField("field", "value", Field.Store.YES));
      indexWriter.addDocument(document);
      indexWriter.close();
   }

   class InboundInvocationHandlerDecorator implements PerCacheInboundInvocationHandler {
      final Set<AbstractDataWriteCommand> writeCommands = new ConcurrentHashSet<>();
      final PerCacheInboundInvocationHandler delegate;

      InboundInvocationHandlerDecorator(PerCacheInboundInvocationHandler delegate) {
         this.delegate = delegate;
      }

      @Override
      public void handle(CacheRpcCommand cmd, Reply reply, DeliverOrder order) {
         if (cmd instanceof SingleRpcCommand) {
            SingleRpcCommand singleRpcCommand = (SingleRpcCommand) cmd;
            ReplicableCommand command = singleRpcCommand.getCommand();
            if (command instanceof AbstractDataWriteCommand) {
               writeCommands.add((AbstractDataWriteCommand) command);
            }
         }
         delegate.handle(cmd, reply, order);
      }
   }

}
