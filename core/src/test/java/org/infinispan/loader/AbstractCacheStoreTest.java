package org.infinispan.loader;

import static org.easymock.classextension.EasyMock.createMock;
import org.infinispan.util.ReflectionUtil;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.ExecutorService;

/**
 * Unit tests that cover {@link  AbstractCacheStoreTest }
 *
 * @author Adrian Cole
 * @version $Id: $
 * @since 4.0
 */
@Test(groups = "unit", testName = "loader.AbstractCacheStoreTest")
public class AbstractCacheStoreTest {
   private AbstractCacheStore cs;
   private AbstractCacheStoreConfig cfg;

   @BeforeMethod
   public void setUp() throws NoSuchMethodException {
      cs = createMock(AbstractCacheStore.class, AbstractCacheStore.class.getMethod("clear"));
      cfg = new AbstractCacheStoreConfig();
      cs.init(cfg, null, null);
   }

   @AfterMethod
   public void tearDown() throws CacheLoaderException {
      cs.stop();
      cs = null;
      cfg = null;
   }

   @Test
   void testSynchExecutorIsSetWhenCfgPurgeSynchIsTrueOnStart() throws Exception {
      cfg.setPurgeSynchronously(true);
      cs.start();
      ExecutorService service = (ExecutorService) ReflectionUtil.getValue(cs, "purgerService");
      assert service instanceof WithinThreadExecutor;
   }

   @Test
   void testASynchExecutorIsDefaultOnStart() throws Exception {
      cs.start();
      ExecutorService service = (ExecutorService) ReflectionUtil.getValue(cs, "purgerService");
      assert !(service instanceof WithinThreadExecutor);
   }
}
