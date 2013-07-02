package org.infinispan.cli;

public interface CommandBuffer {
   void reset();

   boolean addCommand(String command, int nesting);

}
