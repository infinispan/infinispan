package org.infinispan.container.versioning.irac;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BiConsumer;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.container.versioning.InequalVersionComparisonResult;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.ByteString;
import org.infinispan.xsite.XSiteNamedCache;

/**
 * An entry version for the IRAC algorithm (async cross site replication).
 * <p>
 * It is represented as a vector clock where each site keeps it version.
 * <p>
 * The site version is composed as a pair (topology id, version).
 *
 * @author Pedro Ruivo
 * @see TopologyIracVersion
 * @since 11.0
 */
@ProtoTypeId(ProtoStreamTypeIds.IRAC_VERSION)
public class IracEntryVersion {

   private final MapEntry[] vectorClock;

   private IracEntryVersion(MapEntry[] vectorClock) {
      this.vectorClock = vectorClock;
   }

   public static IracEntryVersion newVersion(ByteString site, TopologyIracVersion version) {
      return new IracEntryVersion(new MapEntry[] {new MapEntry(site, version)});
   }

   @ProtoFactory
   static IracEntryVersion protoFactory(List<MapEntry> entries) {
      MapEntry[] vc = entries.toArray(new MapEntry[entries.size()]);
      Arrays.sort(vc);
      return new IracEntryVersion(vc);
   }

   @ProtoField(number = 1, collectionImplementation = ArrayList.class)
   List<MapEntry> entries() {
      return Arrays.asList(vectorClock);
   }

   /**
    * Iterates over all entries of this version as pairs (site name, site version).
    *
    * @param consumer The {@link BiConsumer}.
    */
   public void forEach(BiConsumer<ByteString, TopologyIracVersion> consumer) {
      for (MapEntry entry : vectorClock) {
         consumer.accept(entry.site, entry.version);
      }
   }

   /**
    * Compares this instance with another {@link IracEntryVersion} instance.
    * @param other The other {@link IracEntryVersion} instance.
    * @return A {@link InequalVersionComparisonResult} instance with the compare result.
    */
   public InequalVersionComparisonResult compareTo(IracEntryVersion other) {
      VectorClockComparator comparator = new VectorClockComparator(Math.max(vectorClock.length, other.vectorClock.length));
      forEach(comparator::setOurs);
      other.forEach(comparator::setTheirs);

      Merger merger = Merger.NONE;
      for (VersionCompare v : comparator.values()) {
         merger = merger.accept(v);
      }
      return merger.result();
   }

   public IracEntryVersion merge(IracEntryVersion other) {
      if (other == null) {
         return this;
      }
      TreeMap<ByteString, TopologyIracVersion> copy = toTreeMap(vectorClock);
      for (MapEntry entry : other.vectorClock) {
         copy.merge(entry.site, entry.version, TopologyIracVersion::max);
      }
      return new IracEntryVersion(toMapEntryArray(copy));
   }

   public TopologyIracVersion getVersion(ByteString siteName) {
      int index = Arrays.binarySearch(vectorClock, searchKey(siteName));
      return index >= 0 ? vectorClock[index].version : null;
   }

   public int getTopology(ByteString siteName) {
      TopologyIracVersion version = getVersion(siteName);
      return version == null ? 0 : version.getTopologyId();
   }

   public IracEntryVersion increment(ByteString siteName, int topologyId) {
      TreeMap<ByteString, TopologyIracVersion> map = toTreeMap(vectorClock);
      TopologyIracVersion existing = map.get(siteName);
      if (existing == null) {
         map.put(siteName, TopologyIracVersion.newVersion(topologyId));
      } else {
         map.put(siteName, existing.increment(topologyId));
      }
      return new IracEntryVersion(toMapEntryArray(map));
   }

   @Override
   public String toString() {
      List<String> entries = new LinkedList<>();
      forEach((site, version) -> entries.add(site + "=" + version));
      return "(" + String.join(", ", entries) + ")";
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (o == null || getClass() != o.getClass()) {
         return false;
      }

      IracEntryVersion other = (IracEntryVersion) o;

      return Arrays.equals(vectorClock, other.vectorClock);
   }

   @Override
   public int hashCode() {
      return Arrays.hashCode(vectorClock);
   }

   private static MapEntry[] toMapEntryArray(TreeMap<ByteString, TopologyIracVersion> map) {
      int length = map.size();
      MapEntry[] entries = new MapEntry[length];
      int index = 0;
      for (Map.Entry<ByteString, TopologyIracVersion> e : map.entrySet()) {
         entries[index++] = new MapEntry(e.getKey(), e.getValue());
      }
      return entries;
   }

   private static TreeMap<ByteString, TopologyIracVersion> toTreeMap(MapEntry[] entries) {
      TreeMap<ByteString, TopologyIracVersion> copy = new TreeMap<>();
      for (MapEntry entry : entries) {
         copy.put(entry.site, entry.version);
      }
      return copy;
   }

   private static MapEntry searchKey(ByteString site) {
      return new MapEntry(site, null);
   }

   private enum Merger {
      NONE {
         @Override
         Merger accept(VersionCompare versions) {
            int compare = versions.ours.compareTo(versions.theirs);
            if (compare < 0) {
               return OLD;
            } else if (compare > 0) {
               return NEW;
            }
            return EQUALS;
         }

         @Override
         InequalVersionComparisonResult result() {
            throw new IllegalStateException();
         }
      },
      OLD {
         @Override
         Merger accept(VersionCompare versions) {
            int compare = versions.ours.compareTo(versions.theirs);
            if (compare < 0) {
               return OLD;
            } else if (compare > 0) {
               return CONFLICT;
            }
            return OLD_OR_EQUALS;
         }

         @Override
         InequalVersionComparisonResult result() {
            return InequalVersionComparisonResult.BEFORE;
         }
      },
      OLD_OR_EQUALS {
         @Override
         Merger accept(VersionCompare versions) {
            int compare = versions.ours.compareTo(versions.theirs);
            return compare <= 0 ? OLD_OR_EQUALS : CONFLICT;
         }

         @Override
         InequalVersionComparisonResult result() {
            return InequalVersionComparisonResult.BEFORE;
         }
      },
      NEW {
         @Override
         Merger accept(VersionCompare versions) {
            int compare = versions.ours.compareTo(versions.theirs);
            if (compare > 0) {
               return NEW;
            } else if (compare < 0) {
               return CONFLICT;
            }
            return NEW_OR_EQUALS;
         }

         @Override
         InequalVersionComparisonResult result() {
            return InequalVersionComparisonResult.AFTER;
         }
      },
      NEW_OR_EQUALS {
         @Override
         Merger accept(VersionCompare versions) {
            int compare = versions.ours.compareTo(versions.theirs);
            return compare < 0 ? CONFLICT : NEW_OR_EQUALS;
         }

         @Override
         InequalVersionComparisonResult result() {
            return InequalVersionComparisonResult.AFTER;
         }
      },
      EQUALS {
         @Override
         Merger accept(VersionCompare versions) {
            int compare = versions.ours.compareTo(versions.theirs);
            if (compare < 0) {
               return OLD_OR_EQUALS;
            } else if (compare > 0) {
               return NEW_OR_EQUALS;
            }
            return EQUALS;
         }

         @Override
         InequalVersionComparisonResult result() {
            return InequalVersionComparisonResult.EQUAL;
         }
      },
      CONFLICT {
         @Override
         Merger accept(VersionCompare versions) {
            //no-op
            return CONFLICT;
         }

         @Override
         InequalVersionComparisonResult result() {
            return InequalVersionComparisonResult.CONFLICTING;
         }
      };

      abstract Merger accept(VersionCompare versions);

      abstract InequalVersionComparisonResult result();
   }

   @ProtoTypeId(ProtoStreamTypeIds.IRAC_VERSION_ENTRY)
   public static class MapEntry implements Comparable<MapEntry> {

      final ByteString site;

      @ProtoField(2)
      final TopologyIracVersion version;

      @ProtoFactory
      MapEntry(String site, TopologyIracVersion version) {
         this(XSiteNamedCache.cachedByteString(site), version);
      }

      MapEntry(ByteString site, TopologyIracVersion version) {
         this.site = site;
         this.version = version;
      }

      @ProtoField(1)
      public String getSite() {
         return site.toString();
      }

      @Override
      public String toString() {
         return "MapEntry{" +
               "site='" + site + '\'' +
               ", version=" + version +
               '}';
      }

      @Override
      public int compareTo(MapEntry o) {
         return site.compareTo(o.site);
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         MapEntry entry = (MapEntry) o;

         return site.equals(entry.site) && version.equals(entry.version);
      }

      @Override
      public int hashCode() {
         int result = site.hashCode();
         result = 31 * result + version.hashCode();
         return result;
      }
   }

   private static class VersionCompare {
      TopologyIracVersion ours;
      TopologyIracVersion theirs;

      @Override
      public String toString() {
         return "VersionCompare{" +
               "ours=" + ours +
               ", theirs=" + theirs +
               '}';
      }
   }

   private static class VectorClockComparator {
      private final Map<ByteString, VersionCompare> vectorClock;

      VectorClockComparator(int capacity) {
         vectorClock = new HashMap<>(capacity);
      }

      @Override
      public String toString() {
         return "VectorClock{" +
               "vectorClock=" + vectorClock +
               '}';
      }

      void setOurs(ByteString site, TopologyIracVersion version) {
         VersionCompare v = vectorClock.get(site);
         if (v == null) {
            v = new VersionCompare();
            vectorClock.put(site, v);
         }
         v.ours = version;
         if (v.theirs == null) {
            v.theirs = TopologyIracVersion.NO_VERSION;
         }
      }

      void setTheirs(ByteString site, TopologyIracVersion version) {
         VersionCompare v = vectorClock.get(site);
         if (v == null) {
            v = new VersionCompare();
            vectorClock.put(site, v);
         }
         v.theirs = version;
         if (v.ours == null) {
            v.ours = TopologyIracVersion.NO_VERSION;
         }
      }

      Collection<VersionCompare> values() {
         return vectorClock.values();
      }
   }
}
