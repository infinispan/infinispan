package org.infinispan.container.versioning.irac;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.container.versioning.InequalVersionComparisonResult;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

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

   private final Map<String, TopologyIracVersion> vectorClock;

   public IracEntryVersion(Map<String, TopologyIracVersion> vectorClock) {
      this.vectorClock = Objects.requireNonNull(vectorClock);
   }

   @ProtoFactory
   static IracEntryVersion protoFactory(List<MapEntry> entries) {
      Map<String, TopologyIracVersion> vectorClock = entries.stream()
            .collect(Collectors.toMap(MapEntry::getSite, MapEntry::getVersion));
      return new IracEntryVersion(vectorClock);
   }

   @ProtoField(number = 1, collectionImplementation = ArrayList.class)
   List<MapEntry> entries() {
      List<MapEntry> entries = new ArrayList<>(3);
      vectorClock.forEach((site, version) -> entries.add(new MapEntry(site, version)));
      return entries;
   }

   /**
    * Converts this instance to a {@link Map}.
    * <p>
    * The map cannot be modified!.
    *
    * @return The {@link Map} representation of this version.
    */
   public Map<String, TopologyIracVersion> toMap() {
      return Collections.unmodifiableMap(vectorClock);
   }

   /**
    * Iterates over all entries of this version as pairs (site name, site version).
    *
    * @param consumer The {@link BiConsumer}.
    */
   public void forEach(BiConsumer<String, TopologyIracVersion> consumer) {
      vectorClock.forEach(consumer);
   }

   /**
    * Compares this instance with another {@link IracEntryVersion} instance.
    * @param other The other {@link IracEntryVersion} instance.
    * @return A {@link InequalVersionComparisonResult} instance with the compare result.
    */
   public InequalVersionComparisonResult compareTo(IracEntryVersion other) {
      VectorClock vectorClock = new VectorClock();
      this.forEach(vectorClock::setOurs);
      other.forEach(vectorClock::setTheirs);

      Merger merger = Merger.NONE;
      for (VersionCompare v : vectorClock.values()) {
         merger = merger.accept(v);
      }
      return merger.result();
   }

   @Override
   public String toString() {
      List<String> entries = new LinkedList<>();
      vectorClock.forEach((site, version) -> entries.add(site + "=" + version));
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

      IracEntryVersion version = (IracEntryVersion) o;

      return vectorClock.equals(version.vectorClock);
   }

   @Override
   public int hashCode() {
      return vectorClock.hashCode();
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
   public static class MapEntry {

      @ProtoField(number = 1)
      final String site;
      @ProtoField(number = 2)
      final TopologyIracVersion version;

      @ProtoFactory
      MapEntry(String site, TopologyIracVersion version) {
         this.site = site;
         this.version = version;
      }

      public String getSite() {
         return site;
      }

      public TopologyIracVersion getVersion() {
         return version;
      }

      @Override
      public String toString() {
         return "MapEntry{" +
               "site='" + site + '\'' +
               ", version=" + version +
               '}';
      }
   }

   private static class VersionCompare {
      private TopologyIracVersion ours;
      private TopologyIracVersion theirs;

      @Override
      public String toString() {
         return "VersionCompare{" +
               "ours=" + ours +
               ", theirs=" + theirs +
               '}';
      }
   }

   private static class VectorClock {
      private final Map<String, VersionCompare> vectorClock;

      private VectorClock() {
         vectorClock = new HashMap<>();
      }

      @Override
      public String toString() {
         return "VectorClock{" +
               "vectorClock=" + vectorClock +
               '}';
      }

      void setOurs(String site, TopologyIracVersion version) {
         VersionCompare v = vectorClock.get(site);
         if (v == null) {
            v = new VersionCompare();
            vectorClock.put(site, v);
         }
         v.ours = version;
         if (v.theirs == null) {
            v.theirs = new TopologyIracVersion(0, 0);
         }
      }

      void setTheirs(String site, TopologyIracVersion version) {
         VersionCompare v = vectorClock.get(site);
         if (v == null) {
            v = new VersionCompare();
            vectorClock.put(site, v);
         }
         v.theirs = version;
         if (v.ours == null) {
            v.ours = new TopologyIracVersion(0, 0);
         }
      }

      Collection<VersionCompare> values() {
         return vectorClock.values();
      }
   }
}
