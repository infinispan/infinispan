package org.infinispan.lucene.profiling;

import java.io.IOException;

import org.apache.lucene.store.Directory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * LuceneUserThread: base class to perform activities on the index, as searching, adding to index and
 * deleting.
 *
 * @author Sanne Grinovero
 * @since 4.0
 */
public abstract class LuceneUserThread implements Runnable {

   private static final Log log = LogFactory.getLog(LuceneUserThread.class);

   protected final Directory directory;
   protected final SharedState state;

   LuceneUserThread(Directory dir, SharedState state) {
      this.directory = dir;
      this.state = state;
   }

   @Override
   public final void run() {
      try {
         state.waitForStart();
      } catch (InterruptedException e1) {
         state.errorManage(e1);
         return;
      }
      while (!state.needToQuit()) {
         try {
            testLoop();
         } catch (Exception e) {
            log.error("unexpected error", e);
            state.errorManage(e);
         }
      }
      try {
         cleanup();
      } catch (IOException e) {
         log.error("unexpected error", e);
         state.errorManage(e);
      }
   }

   protected abstract void testLoop() throws IOException;

   protected void cleanup() throws IOException {
      // defaults to no operation
   }

}
