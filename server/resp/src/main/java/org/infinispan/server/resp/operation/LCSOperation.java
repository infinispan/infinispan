package org.infinispan.server.resp.operation;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.AdvancedCache;
import org.infinispan.server.resp.response.LCSResponse;

public class LCSOperation {

   private static final byte[] EMPTY_ARRAY = new byte[0];
   private static final CompletionStage<LCSResponse> MISSING_ARGUMENTS = CompletableFuture
         .failedFuture(new IllegalStateException("Missing arguments"));

   public static CompletionStage<LCSResponse> performOperation(AdvancedCache<byte[], byte[]> cache,
         List<byte[]> arguments) {
      if (arguments.size() < 4)
         return MISSING_ARGUMENTS;
      LCSOperationContext lcsCtx = new LCSOperationContext(arguments);
      lcsCtx.cache = cache;
      return lcsCtx.cache.getAllAsync(Set.of(lcsCtx.key1, lcsCtx.key2))
            .thenApply((m) -> {
               if (m.size() < 2) {
                  lcsCtx.result = new LCSResponse();
                  lcsCtx.result.lcs = EMPTY_ARRAY;
               }

               var v1 = findValue(m, lcsCtx.key1);
               var v2 = findValue(m, lcsCtx.key2);
               if (v1 == null || v2 == null) {
                  lcsCtx.result = new LCSResponse();
                  lcsCtx.result.lcs = EMPTY_ARRAY;
               }

               lcsCtx.lcsLength(v1, v2);
               if (!lcsCtx.justLen) {
                  lcsCtx.backtrack(v1, v2);
               }
               return lcsCtx.result;
            });
   }

   private static byte[] findValue(Map<byte[], byte[]> values, byte[] key) {
      assert values.size() == 2;
      for (Map.Entry<byte[], byte[]> entry : values.entrySet()) {
         if (Arrays.equals(entry.getKey(), key)) {
            return entry.getValue();
         }
      }
      return null;
   }

   protected static class LCSOperationContext {
      private final List<byte[]> arguments;
      AdvancedCache<byte[], byte[]> cache;
      private byte[] key1;
      private byte[] key2;
      private boolean justLen;
      private boolean idx;
      private boolean matchLen;
      private long minMatchLen;

      private LCSResponse result;

      public LCSResponse getResult() {
         return result;
      }

      public LCSOperationContext(byte[] v1, byte[] v2, boolean onlyLen, boolean idx, boolean matchLen,
            long minMatchLen) {
         this.arguments = null;
         this.justLen = onlyLen;
         this.idx = idx;
         this.matchLen = matchLen;
         this.minMatchLen = minMatchLen;
         this.result = new LCSResponse();
      }

      public LCSOperationContext(List<byte[]> arguments) {
         this.arguments = arguments;
         this.key1 = null;
         this.key2 = null;
         this.matchLen = false;
         this.justLen = false;
         this.minMatchLen = 0;
         this.result = new LCSResponse();
         parseAndLoadOptions();
      }

      private void parseAndLoadOptions() {
         if (!(new String(arguments.get(0), StandardCharsets.US_ASCII)).equals("LCS")) {
            throw new IllegalArgumentException("Unknown argument for LCS operation");
         }
         if (!(new String(arguments.get(1), StandardCharsets.US_ASCII)).equals("KEYS")) {
            throw new IllegalArgumentException("Unknown argument for LCS operation");
         }
         this.key1 = arguments.get(2);
         this.key2 = arguments.get(3);

         // Below here we parse the optional arguments for the LCS command:
         //
         // LEN: returns the length of the longest match
         // IDX: returns the index position of each matching excludes LEN)
         // MINMATCHLEN: returns indexes only for matching longer than
         // WITHMATCHLEN: returns length of each match
         for (int i = 4; i < arguments.size(); i++) {
            byte[] arg = arguments.get(i);
            switch (new String(arg, StandardCharsets.US_ASCII)) {
               case "LEN":
                  if (this.idx)
                     throw new IllegalArgumentException(
                           "ERR If you want both the length and indexes, please just use IDX.");
                  this.justLen = true;
                  continue;
               case "IDX":
                  if (this.matchLen)
                     throw new IllegalArgumentException(
                           "ERR If you want both the length and indexes, please just use IDX.");
                  idx = true;
                  continue;
               case "MINMATCHLEN":
                  if (i + 1 > arguments.size())
                     throw new IllegalArgumentException("ERR syntax error");
                  this.minMatchLen = Long.parseLong(new String(arguments.get(i + 1), StandardCharsets.US_ASCII));
                  i++;
                  continue;
               case "WITHMATCHLEN":
                  if (this.matchLen)
                     throw new IllegalArgumentException(
                           "ERR If you want both the length and indexes, please just use IDX.");
                  matchLen = idx; // matchLen useless without idx
                  continue;
            }
            throw new IllegalArgumentException("Unknown argument for LCS operation");
         }
      }

      // See https://en.wikipedia.org/wiki/Longest_common_subsequence
      // Keeping same naming for reference
      public void lcsLength(byte[] a, byte[] b) {
         int m = a.length;
         int n = b.length;
         var C = new int[m + 1][n + 1];
         for (int i = 1; i <= m; i++) {
            for (int j = 1; j <= n; j++) {
               if (a[i - 1] == b[j - 1]) {
                  C[i][j] = C[i - 1][j - 1] + 1;
               } else {
                  C[i][j] = Math.max(C[i][j - 1], C[i - 1][j]);
               }
            }
         }
         this.result.C = C;
         this.result.len = C[m][n];
      }

      // This backtrack from bottom right of the C matrix to top left
      // see example in link above
      public void backtrack(byte[] aStr, byte[] bStr) {
         int x = aStr.length;
         int y = bStr.length;
         int i = x;
         int j = y;
         int m = this.result.len - 1;
         boolean matching = false;
         // If idx we need only the matching index
         // else we need only the lcs string
         if (this.idx) {
            this.result.idx = new ArrayList<long[]>();
         } else {
            this.result.lcs = new byte[this.result.len];
         }
         while (i > 0 && j > 0) {
            if (aStr[i - 1] == bStr[j - 1]) { // On match, save char and move up-left
               matching = true;
               if (!this.idx) {
                  this.result.lcs[m--] = aStr[i - 1];
               }
               i--;
               j--;
            } else { // on not match
               if (matching) { // if matching, no more matching and save match positions
                  matching = false;
                  if (x - i >= this.minMatchLen && this.idx) {
                     if (this.matchLen) {
                        this.result.idx.add(new long[] { i, x - 1, j, y - 1, x - i });
                     } else {
                        this.result.idx.add(new long[] { i, x - 1, j, y - 1 });
                     }
                  }
                  x = i;
                  y = j;
               }
               // Decide where to go next
               if (this.result.C[i][j - 1] >= this.result.C[i - 1][j]) {
                  // go left ...
                  y--;
                  j--;
               } else {
                  // ... or go up
                  x--;
                  i--;
               }
            }
         }
         if (matching && this.idx && x - i >= this.minMatchLen) {
            if (this.matchLen) {
               this.result.idx.add(new long[] { i, x - 1, j, y - 1, x - i });
            } else {
               this.result.idx.add(new long[] { i, x - 1, j, y - 1 });
            }
         }
      }
   }
}
