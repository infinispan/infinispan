package org.horizon.eviction.algorithms;

import org.easymock.EasyMock;
import static org.easymock.EasyMock.*;
import org.horizon.container.DataContainer;
import org.horizon.eviction.EvictionAction;
import org.horizon.eviction.EvictionAlgorithm;
import org.horizon.eviction.EvictionAlgorithmConfig;
import org.horizon.eviction.events.EvictionEvent;
import static org.horizon.eviction.events.EvictionEvent.Type.*;
import org.horizon.eviction.events.InUseEvictionEvent;
import org.horizon.util.Util;
import org.testng.annotations.Test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@Test(groups = "unit")
public abstract class BaseAlgorithmTest {

   protected abstract BaseEvictionAlgorithmConfig getNewEvictionAlgorithmConfig();

   protected BaseEvictionAlgorithm createAndInit(EvictionAlgorithmConfig cfg) throws Exception {
      DataContainer dc = createNiceMock(DataContainer.class);
      expect(dc.getModifiedTimestamp(anyObject())).andReturn(System.currentTimeMillis()).anyTimes();
      replay(dc);
      return createAndInit(cfg, dc);
   }

   protected BaseEvictionAlgorithm createAndInit(EvictionAlgorithmConfig cfg, DataContainer dc) throws Exception {
      BaseEvictionAlgorithm a = (BaseEvictionAlgorithm) Util.getInstance(cfg.getEvictionAlgorithmClassName());
      a.init(null, dc, cfg);
      a.start();
      return a;
   }

   public void testMinTimeToLive() throws Exception {
      BaseEvictionAlgorithmConfig cfg = getNewEvictionAlgorithmConfig();
      cfg.setMinTimeToLive(2 * 60 * 60 * 1000); // something enormous - 2 hrs
      cfg.setMaxEntries(5);
      DataContainer dc = createMock(DataContainer.class);
      expect(dc.getModifiedTimestamp(anyObject())).andReturn(System.currentTimeMillis()).anyTimes();
      replay(dc);
      BaseEvictionAlgorithm a = createAndInit(cfg, dc);
      EvictionAction mockAction = createMock(EvictionAction.class);
      a.setEvictionAction(mockAction);
      BlockingQueue<EvictionEvent> eventQueue = new LinkedBlockingQueue<EvictionEvent>();
      for (int i = 0; i < 10; i++) eventQueue.put(new EvictionEvent(i, EvictionEvent.Type.ADD_ENTRY_EVENT));

      assert eventQueue.size() == 10;

      // what do we expect to happen on the eviction action class?
      // nothing at this stage.
      replay(mockAction);
      a.process(eventQueue);
      verify(mockAction);


      reset(dc);
      for (Object k : a.getEvictionQueue()) {
         // change the creation timestamp to before 2 hrs in the past
         // for all even keys
         Integer key = (Integer) k;

         if (key % 2 == 0) {
            expect(dc.getModifiedTimestamp(eq(key))).andReturn((long) 1).once();
            EvictionEvent e = new EvictionEvent(key, EvictionEvent.Type.VISIT_ENTRY_EVENT);
            eventQueue.put(e);
         } else {
            expect(dc.getModifiedTimestamp(eq(key))).andReturn(System.currentTimeMillis()).once();
            eventQueue.put(new EvictionEvent(key, EvictionEvent.Type.VISIT_ENTRY_EVENT));
         }
      }

      assert eventQueue.size() == 10;

      // this time we expect all even numbered keys to get evicted.
      reset(mockAction);
      expect(mockAction.evict(eq(0))).andReturn(true).once();
      expect(mockAction.evict(eq(2))).andReturn(true).once();
      expect(mockAction.evict(eq(4))).andReturn(true).once();
      expect(mockAction.evict(eq(6))).andReturn(true).once();
      expect(mockAction.evict(eq(8))).andReturn(true).once();
      replay(mockAction, dc);
      a.process(eventQueue);
      verify(mockAction);
   }

   protected boolean timeOrderedQueue() {
      return true;
   }

   protected boolean reverseOrder() {
      return false;
   }

   public void testNumEntries1() throws Exception {
      BaseEvictionAlgorithmConfig config = getNewEvictionAlgorithmConfig();
      config.setMaxEntries(4);
      config.setMinEntries(2);
      BlockingQueue<EvictionEvent> eventQueue = new LinkedBlockingQueue<EvictionEvent>();
      eventQueue.put(new EvictionEvent("one", ADD_ENTRY_EVENT));
      eventQueue.put(new EvictionEvent("two", ADD_ENTRY_EVENT));
      eventQueue.put(new EvictionEvent("three", ADD_ENTRY_EVENT));
      eventQueue.put(new EvictionEvent("four", ADD_ENTRY_EVENT));
      eventQueue.put(new EvictionEvent("five", ADD_ENTRY_EVENT));
      EvictionAlgorithm algo = createAndInit(config);
      EvictionAction mockAction = createMock(EvictionAction.class);
      algo.setEvictionAction(mockAction);

      // should prune down to 2 entries
      if (timeOrderedQueue()) {
         if (reverseOrder()) {
            expect(mockAction.evict(eq("five"))).andReturn(true).once();
            expect(mockAction.evict(eq("four"))).andReturn(true).once();
            expect(mockAction.evict(eq("three"))).andReturn(true).once();
         } else {
            expect(mockAction.evict(eq("one"))).andReturn(true).once();
            expect(mockAction.evict(eq("two"))).andReturn(true).once();
            expect(mockAction.evict(eq("three"))).andReturn(true).once();
         }
      } else {
         expect(mockAction.evict(anyObject())).andReturn(true).times(3);
      }
      replay(mockAction);
      algo.process(eventQueue);
      verify(mockAction);

   }

   public void testNumEntries2() throws Exception {
      BaseEvictionAlgorithmConfig config = getNewEvictionAlgorithmConfig();
      config.setMaxEntries(0);
      config.setMinEntries(20);
      BlockingQueue<EvictionEvent> eventQueue = new LinkedBlockingQueue<EvictionEvent>();
      eventQueue.put(new EvictionEvent("one", ADD_ENTRY_EVENT));
      eventQueue.put(new EvictionEvent("two", ADD_ENTRY_EVENT));
      EvictionAlgorithm algo = createAndInit(config);
      EvictionAction mockAction = createMock(EvictionAction.class);
      algo.setEvictionAction(mockAction);

      // should prune down to 2 entries
      EasyMock.expect(mockAction.evict(eq("one"))).andReturn(true).once();
      EasyMock.expect(mockAction.evict(eq("two"))).andReturn(true).once();
      EasyMock.replay(mockAction);
      algo.process(eventQueue);
      EasyMock.verify(mockAction);
   }

   public void testNumEntries3() throws Exception {
      BaseEvictionAlgorithmConfig config = getNewEvictionAlgorithmConfig();
      config.setMaxEntries(3);
      config.setMinEntries(20);
      BlockingQueue<EvictionEvent> eventQueue = new LinkedBlockingQueue<EvictionEvent>();
      eventQueue.put(new EvictionEvent("one", ADD_ENTRY_EVENT));
      eventQueue.put(new EvictionEvent("two", ADD_ENTRY_EVENT));
      eventQueue.put(new EvictionEvent("three", ADD_ENTRY_EVENT));
      eventQueue.put(new EvictionEvent("four", ADD_ENTRY_EVENT));
      EvictionAlgorithm algo = createAndInit(config);
      EvictionAction mockAction = createMock(EvictionAction.class);
      algo.setEvictionAction(mockAction);

      // should prune down to equal to maxEntries
      if (timeOrderedQueue()) {
         if (reverseOrder()) {
            EasyMock.expect(mockAction.evict(eq("four"))).andReturn(true).once();
         } else {
            EasyMock.expect(mockAction.evict(eq("one"))).andReturn(true).once();
         }
      } else {
         EasyMock.expect(mockAction.evict(anyObject())).andReturn(true).once();
      }

      EasyMock.replay(mockAction);
      algo.process(eventQueue);
      EasyMock.verify(mockAction);
   }

   public void testNumEntries4() throws Exception {
      BaseEvictionAlgorithmConfig config = getNewEvictionAlgorithmConfig();
      config.setMaxEntries(-1);
      config.setMinEntries(2);
      BlockingQueue<EvictionEvent> eventQueue = new LinkedBlockingQueue<EvictionEvent>();
      eventQueue.put(new EvictionEvent("one", ADD_ENTRY_EVENT));
      eventQueue.put(new EvictionEvent("two", ADD_ENTRY_EVENT));
      eventQueue.put(new EvictionEvent("three", ADD_ENTRY_EVENT));
      eventQueue.put(new EvictionEvent("four", ADD_ENTRY_EVENT));
      EvictionAlgorithm algo = createAndInit(config);
      EvictionAction mockAction = createMock(EvictionAction.class);
      algo.setEvictionAction(mockAction);

      // expecting nothing to be evicted
      EasyMock.replay(mockAction);
      algo.process(eventQueue);
      EasyMock.verify(mockAction);
   }

   public void testNumEntries5() throws Exception {
      BaseEvictionAlgorithmConfig config = getNewEvictionAlgorithmConfig();
      config.setMaxEntries(3);
      config.setMinEntries(-1);
      BlockingQueue<EvictionEvent> eventQueue = new LinkedBlockingQueue<EvictionEvent>();
      eventQueue.put(new EvictionEvent("one", ADD_ENTRY_EVENT));
      eventQueue.put(new EvictionEvent("two", ADD_ENTRY_EVENT));
      eventQueue.put(new EvictionEvent("three", ADD_ENTRY_EVENT));
      eventQueue.put(new EvictionEvent("four", ADD_ENTRY_EVENT));
      EvictionAlgorithm algo = createAndInit(config);
      EvictionAction mockAction = createMock(EvictionAction.class);
      algo.setEvictionAction(mockAction);

      // should prune down to equal to maxEntries
      if (timeOrderedQueue()) {
         if (reverseOrder()) {
            EasyMock.expect(mockAction.evict(eq("four"))).andReturn(true).once();
         } else {
            EasyMock.expect(mockAction.evict(eq("one"))).andReturn(true).once();
         }
      } else {
         EasyMock.expect(mockAction.evict(anyObject())).andReturn(true).once();
      }
      EasyMock.replay(mockAction);
      algo.process(eventQueue);
      EasyMock.verify(mockAction);
   }

   public void testRemoveEvent() throws Exception {
      BaseEvictionAlgorithmConfig config = getNewEvictionAlgorithmConfig();
      config.setMinTimeToLive(24 * 60 * 60, TimeUnit.SECONDS);  // huge - so evictions wont happen
      BlockingQueue<EvictionEvent> eventQueue = new LinkedBlockingQueue<EvictionEvent>();
      eventQueue.put(new EvictionEvent("one", ADD_ENTRY_EVENT));
      eventQueue.put(new EvictionEvent("two", ADD_ENTRY_EVENT));
      eventQueue.put(new EvictionEvent("three", ADD_ENTRY_EVENT));
      eventQueue.put(new EvictionEvent("four", ADD_ENTRY_EVENT));
      BaseEvictionAlgorithm algo = createAndInit(config);
      EvictionAction mockAction = EasyMock.createNiceMock(EvictionAction.class);
      algo.setEvictionAction(mockAction);

      // should prune down to equal to maxEntries
      EasyMock.expect(mockAction.evict(eq("one"))).andReturn(true).once();
      EasyMock.replay(mockAction);
      algo.process(eventQueue);

      assert algo.evictionQueue.size() == 4;

      eventQueue.put(new EvictionEvent("three", REMOVE_ENTRY_EVENT));
      algo.process(eventQueue);
      assert algo.getEvictionQueue().size() == 3;
      assert !algo.getEvictionQueue().contains("three");
   }

   public void testClearEvent() throws Exception {
      BaseEvictionAlgorithmConfig config = getNewEvictionAlgorithmConfig();
      config.setMinTimeToLive(24 * 60 * 60, TimeUnit.SECONDS);  // huge - so evictions wont happen
      BlockingQueue<EvictionEvent> eventQueue = new LinkedBlockingQueue<EvictionEvent>();
      eventQueue.put(new EvictionEvent("one", ADD_ENTRY_EVENT));
      eventQueue.put(new EvictionEvent("two", ADD_ENTRY_EVENT));
      eventQueue.put(new EvictionEvent("three", ADD_ENTRY_EVENT));
      eventQueue.put(new EvictionEvent("four", ADD_ENTRY_EVENT));
      BaseEvictionAlgorithm algo = createAndInit(config);
      EvictionAction mockAction = EasyMock.createNiceMock(EvictionAction.class);
      algo.setEvictionAction(mockAction);

      // should prune down to equal to maxEntries
      EasyMock.expect(mockAction.evict(eq("one"))).andReturn(true).once();
      EasyMock.replay(mockAction);
      algo.process(eventQueue);

      assert algo.getEvictionQueue().size() == 4;

      eventQueue.put(new EvictionEvent("three", CLEAR_CACHE_EVENT));
      algo.process(eventQueue);
      assert algo.getEvictionQueue().size() == 0;
      assert !algo.getEvictionQueue().contains("three");
   }

   public void testMarkInUseControl() throws Exception {
      BaseEvictionAlgorithmConfig config = getNewEvictionAlgorithmConfig();
      config.setMaxEntries(1);
      BlockingQueue<EvictionEvent> eventQueue = new LinkedBlockingQueue<EvictionEvent>();
      eventQueue.put(new EvictionEvent("one", ADD_ENTRY_EVENT));
      eventQueue.put(new EvictionEvent("two", ADD_ENTRY_EVENT));
      BaseEvictionAlgorithm algo = createAndInit(config);
      EvictionAction mockAction = EasyMock.createNiceMock(EvictionAction.class);
      algo.setEvictionAction(mockAction);

      // should prune down to equal to maxEntries
      if (timeOrderedQueue()) {
         if (reverseOrder()) {
            expect(mockAction.evict(eq("two"))).andReturn(true).once();
         } else {
            expect(mockAction.evict(eq("one"))).andReturn(true).once();
         }
      } else {
         expect(mockAction.evict(anyObject())).andReturn(true).once();
      }

      replay(mockAction);
      algo.process(eventQueue);
      verify(mockAction);

      assert algo.keysInUse.isEmpty();
   }

   public void testMarkInUse() throws Exception {
      BaseEvictionAlgorithmConfig config = getNewEvictionAlgorithmConfig();
      config.setMaxEntries(1);
      BlockingQueue<EvictionEvent> eventQueue = new LinkedBlockingQueue<EvictionEvent>();
      eventQueue.put(new EvictionEvent("one", ADD_ENTRY_EVENT));
      eventQueue.put(new EvictionEvent("two", ADD_ENTRY_EVENT));
      eventQueue.put(new InUseEvictionEvent("one", 200000));
      eventQueue.put(new InUseEvictionEvent("two", 200000));
      BaseEvictionAlgorithm algo = createAndInit(config);
      EvictionAction mockAction = EasyMock.createNiceMock(EvictionAction.class);
      algo.setEvictionAction(mockAction);

      // Nothing should happen
      replay(mockAction);
      algo.process(eventQueue);
      verify(mockAction);

      assert algo.keysInUse.size() == 2;

      eventQueue = new LinkedBlockingQueue<EvictionEvent>();
      eventQueue.put(new EvictionEvent("one", UNMARK_IN_USE_EVENT));
      eventQueue.put(new EvictionEvent("two", UNMARK_IN_USE_EVENT));

      reset(mockAction);
      // should prune down to equal to maxEntries
      if (timeOrderedQueue()) {
         if (reverseOrder()) {
            expect(mockAction.evict(eq("two"))).andReturn(true).once();
         } else {
            expect(mockAction.evict(eq("one"))).andReturn(true).once();
         }
      } else {
         expect(mockAction.evict(anyObject())).andReturn(true).once();
      }

      replay(mockAction);
      algo.process(eventQueue);
      verify(mockAction);

      assert algo.keysInUse.isEmpty();
   }
}
