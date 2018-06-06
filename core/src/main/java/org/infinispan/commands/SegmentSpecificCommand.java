package org.infinispan.commands;

import org.infinispan.distribution.ch.KeyPartitioner;

/**
 * Interface to be implemented when the command can define a single segment for its operation. This is useful
 * so that subsequent operations requiring a segment can retrieve it from the command and have it only computed
 * once at creation time.
 * <p>
 * If a command implements this interface, the command <b>MUST</b> ensure that it is initialized properly to
 * always return a number 0 or greater when invoking {@link #getSegment()}.
 * @author wburns
 * @since 9.3
 */
public interface SegmentSpecificCommand {
   /**
    * Returns the segment that this key maps to. This must always return a number 0 or larger.
    * @return the segment of the key
    */
   int getSegment();

   /**
    * Utility to extract the segment from a given command that may be a {@link SegmentSpecificCommand}. If the
    * command is a {@link SegmentSpecificCommand}, it will immediately return the value from {@link #getSegment()}.
    * Otherwise it will return the result from invoking {@link KeyPartitioner#getSegment(Object)} passing the provided key.
    * @param command the command to extract the segment from
    * @param key the key the segment belongs to
    * @param keyPartitioner the partitioner to calculate the segment of the key
    * @return the segment value to use.
    */
   static int extractSegment(ReplicableCommand command, Object key, KeyPartitioner keyPartitioner) {
      if (command instanceof SegmentSpecificCommand) {
         return ((SegmentSpecificCommand) command).getSegment();
      }
      return keyPartitioner.getSegment(key);
   }
}
