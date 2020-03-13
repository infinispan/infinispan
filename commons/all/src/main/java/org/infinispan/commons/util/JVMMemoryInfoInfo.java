package org.infinispan.commons.util;

import java.lang.management.BufferPoolMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @since 10.0
 */
@SuppressWarnings("unused")
public final class JVMMemoryInfoInfo {

   private static MemoryMXBean memoryMBean;
   private static List<MemoryPoolMXBean> memoryPoolMBeans;
   private static List<BufferPoolMXBean> bufferPoolsMBeans;
   private static List<GarbageCollectorMXBean> garbageCollectorMXBeans;

   public JVMMemoryInfoInfo() {
      garbageCollectorMXBeans = ManagementFactory.getGarbageCollectorMXBeans();
      memoryMBean = ManagementFactory.getMemoryMXBean();
      memoryPoolMBeans = ManagementFactory.getMemoryPoolMXBeans();
      bufferPoolsMBeans = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
   }

   public List<MemoryManager> getGc() {
      return garbageCollectorMXBeans.stream().map(MemoryManager::new).collect(Collectors.toList());
   }

   public List<MemoryPool> getMemoryPools() {
      return memoryPoolMBeans.stream().map(MemoryPool::new).collect(Collectors.toList());
   }

   public List<BufferPool> getBufferPools() {
      return bufferPoolsMBeans.stream().map(BufferPool::new).collect(Collectors.toList());
   }

   public MemoryUsage getHeap() {
      return memoryMBean.getHeapMemoryUsage();
   }

   public MemoryUsage getNonHeap() {
      return memoryMBean.getNonHeapMemoryUsage();
   }

   private static class BufferPool {
      private final long memoryUsed;
      private final String name;
      private final long totalCapacity;
      private final long count;

      BufferPool(BufferPoolMXBean bufferPoolMXBean) {
         memoryUsed = bufferPoolMXBean.getMemoryUsed();
         name = bufferPoolMXBean.getName();
         totalCapacity = bufferPoolMXBean.getTotalCapacity();
         count = bufferPoolMXBean.getCount();
      }

      public long getMemoryUsed() {
         return memoryUsed;
      }

      public String getName() {
         return name;
      }

      public long getTotalCapacity() {
         return totalCapacity;
      }

      public long getCount() {
         return count;
      }
   }

   private static class MemoryPool {
      private final String name;
      private final MemoryType type;
      private final MemoryUsage usage;
      private final MemoryUsage peakUsage;

      MemoryPool(MemoryPoolMXBean memoryPoolMXBean) {
         this.name = memoryPoolMXBean.getName();
         this.type = memoryPoolMXBean.getType();
         this.usage = memoryPoolMXBean.getUsage();
         this.peakUsage = memoryPoolMXBean.getPeakUsage();
      }

      public String getName() {
         return name;
      }

      public MemoryType getType() {
         return type;
      }

      public MemoryUsage getUsage() {
         return usage;
      }

      public MemoryUsage getPeakUsage() {
         return peakUsage;
      }
   }

   private static class MemoryManager {
      private final String name;
      private final String[] memoryPoolNames;
      private final boolean valid;
      private final long collectionCount;
      private final long collectionTime;

      MemoryManager(GarbageCollectorMXBean memoryManagerMXBean) {
         name = memoryManagerMXBean.getName();
         memoryPoolNames = memoryManagerMXBean.getMemoryPoolNames();
         valid = memoryManagerMXBean.isValid();
         collectionCount = memoryManagerMXBean.getCollectionCount();
         collectionTime = memoryManagerMXBean.getCollectionTime();
      }

      public long getCollectionCount() {
         return collectionCount;
      }

      public long getCollectionTime() {
         return collectionTime;
      }

      public String getName() {
         return name;
      }

      public String[] getMemoryPoolNames() {
         return memoryPoolNames;
      }

      public boolean isValid() {
         return valid;
      }
   }

}
