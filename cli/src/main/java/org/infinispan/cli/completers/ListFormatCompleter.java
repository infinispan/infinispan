package org.infinispan.cli.completers;

import org.infinispan.cli.resources.Resource;

public class ListFormatCompleter extends EnumCompleter<Resource.ListFormat> {

   public ListFormatCompleter() {
      super(Resource.ListFormat.class);
   }
}
