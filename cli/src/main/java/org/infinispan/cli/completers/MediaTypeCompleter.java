package org.infinispan.cli.completers;

/**
 * @since 14.0
 */
public class MediaTypeCompleter extends EnumCompleter<MediaTypeCompleter.MediaType> {
   public enum MediaType {
      XML,
      JSON,
      YAML
   }

   public MediaTypeCompleter() {
      super(MediaType.class);
   }
}
