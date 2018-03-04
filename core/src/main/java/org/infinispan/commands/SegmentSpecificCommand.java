package org.infinispan.commands;

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
    * The value used to symbolize that a segment is unknown. This value cannot be returned from {@link #getSegment()}.
    */
   int UNKNOWN_SEGMENT = -1;

   /**
    * Utility to extract the segment from a given command that may be a {@link SegmentSpecificCommand}. If it is
    * it will return the 0 or larger segment. If not it will return {@link #UNKNOWN_SEGMENT} to signify this.
    * @param command the command to extract the segment from
    * @return the segment value from the command, being 0 or greater or {@link #UNKNOWN_SEGMENT} if this command doesn't
    * implement {@link SegmentSpecificCommand}.
    */
   static int extractSegment(ReplicableCommand command) {
      if (command instanceof SegmentSpecificCommand) {
         return ((SegmentSpecificCommand) command).getSegment();
      }
      return UNKNOWN_SEGMENT;
   }
}
