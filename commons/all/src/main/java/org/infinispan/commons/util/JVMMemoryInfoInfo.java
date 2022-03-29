package org.infinispan.commons.util;

import java.io.IOException;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;

import com.sun.management.HotSpotDiagnosticMXBean;

/**
 * @since 10.0
 */
@SuppressWarnings("unused")
public final class JVMMemoryInfoInfo implements JsonSerialization {

   private final MemoryMXBean memoryMBean;
   private final List<MemoryPoolMXBean> memoryPoolMBeans;
   private final List<BufferPoolMXBean> bufferPoolsMBeans;
   private final List<GarbageCollectorMXBean> garbageCollectorMXBeans;
   HotSpotDiagnosticMXBean hotSpotDiagnosticMXBean;

   public JVMMemoryInfoInfo() {
      garbageCollectorMXBeans = ManagementFactory.getGarbageCollectorMXBeans();
      memoryMBean = ManagementFactory.getMemoryMXBean();
      memoryPoolMBeans = ManagementFactory.getMemoryPoolMXBeans();
      bufferPoolsMBeans = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
      hotSpotDiagnosticMXBean = ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class);
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

   public void heapDump(Path path, boolean live) throws IOException {
      hotSpotDiagnosticMXBean.dumpHeap(path.toString(), live);
   }

   private static Json asJson(MemoryUsage usage) {
      return Json.object("init", usage.getInit())
            .set("used", usage.getUsed())
            .set("committed", usage.getCommitted())
            .set("max", usage.getMax());
   }

   @Override
   public Json toJson() {
      return Json.object()
            .set("memory_pools", Json.make(getMemoryPools()))
            .set("gc", Json.make(getGc()))
            .set("buffer_pools", Json.make(getBufferPools()))
            .set("heap", asJson(getHeap()))
            .set("non_heap", asJson(getNonHeap()));
   }

   private static class BufferPool implements JsonSerialization {
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

      @Override
      public Json toJson() {
         return Json.object()
               .set("name", name)
               .set("memory_used", memoryUsed)
               .set("total_capacity", totalCapacity)
               .set("count", count);
      }
   }

   private static class MemoryPool implements JsonSerialization {
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

      @Override
      public Json toJson() {
         return Json.object()
               .set("name", name)
               .set("type", type)
               .set("usage", asJson(usage))
               .set("peak_usage", asJson(peakUsage));
      }
   }

   private static class MemoryManager implements JsonSerialization {
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

      @Override
      public Json toJson() {
         return Json.object()
               .set("name", name)
               .set("valid", valid)
               .set("collection_count", collectionCount)
               .set("collection_time", collectionTime)
               .set("memory_pool_names", Json.make(getMemoryPoolNames()));
      }
   }

}
