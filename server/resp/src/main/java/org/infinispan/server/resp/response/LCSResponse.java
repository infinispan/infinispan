package org.infinispan.server.resp.response;

import java.util.ArrayList;

public class LCSResponse {
      public ArrayList<long[]> idx;
      public byte[] lcs;
      public int[][] C;
      public int len;
   }
