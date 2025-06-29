package com.cypherlabs.io;

import com.cypherlabs.crawler.Token;

import java.io.*;
import java.nio.file.Path;
import java.util.*;

public class PostingsUtils {

    /**
     * Writes the postings list to a binary file in the following format:
     *
     * For each token:
     *   - Writes the number of associated document IDs as an integer (4 bytes)
     *   - Writes each document ID as a 4-byte integer
     *
     * The order of tokens is not guaranteed unless the input map is sorted.
     * The method also tracks and returns the byte offset for each token's
     * entry in the postings file, which is useful for building the token dictionary.
     *
     * @param tokenByDocs  A map of tokens to the set of document IDs in which they appear
     * @param segmentDir   The path to the output directory
     * @param compact Flag for writing compactly using (delta+var int) or with fixed byte length
     * @return A map of tokens to their starting byte offset within the output file
     * @throws IOException If an I/O error occurs during writing
     */
    public static Map<Token, Long> writePostings(Map<Token, Set<Integer>> tokenByDocs, Path segmentDir, boolean compact) throws IOException {
        if(compact)
            return compact(tokenByDocs, segmentDir.resolve("postings.bin"));
        else
            return fixedLength(tokenByDocs, segmentDir.resolve("postings.bin"));
    }


    private static Map<Token, Long> fixedLength(Map<Token, Set<Integer>> tokenByDocs, Path outputFile) throws IOException {
        Map<Token, Long> tokenByOffSet = new HashMap<>();
        int NUM_BYTES_PER_WRITE = Integer.BYTES;
        try(DataOutputStream opStr = new DataOutputStream(new FileOutputStream(outputFile.toFile()))) {
            long offsetAccumulator = 0;
            for(Map.Entry<Token, Set<Integer>> tokenByDocIds : tokenByDocs.entrySet()) {
                tokenByOffSet.put(tokenByDocIds.getKey(), offsetAccumulator);
                List<Integer> sortedDocIds = new ArrayList(tokenByDocIds.getValue());
                Collections.sort(sortedDocIds);
                opStr.writeInt(sortedDocIds.size());
                offsetAccumulator += NUM_BYTES_PER_WRITE;
                for(Integer docId : sortedDocIds) {
                    opStr.writeInt(docId);
                    offsetAccumulator += NUM_BYTES_PER_WRITE;
                }
            }
        }

        return tokenByOffSet;
    }

    private static Map<Token, Long> compact(Map<Token, Set<Integer>> tokenByDocs, Path outputFile) throws IOException {
        Map<Token, Long> tokenByOffSet = new HashMap<>();

        try (CountingOutputStream cos = new CountingOutputStream(new FileOutputStream(outputFile.toFile()));
             DataOutputStream opStr = new DataOutputStream(cos)) {

            for (Map.Entry<Token, Set<Integer>> entry : tokenByDocs.entrySet()) {
                tokenByOffSet.put(entry.getKey(), cos.getCount());
                List<Integer> sortedDocIds = new ArrayList<>(entry.getValue());
                Collections.sort(sortedDocIds);
                writeVarInt(sortedDocIds.size(), opStr);
                int prevDocId = 0;
                for (int docId : sortedDocIds) {
                    int delta = docId - prevDocId;
                    writeVarInt(delta, opStr);
                    prevDocId = docId;
                }
            }
        }

        return tokenByOffSet;
    }

    private static void writeVarInt(int value, DataOutputStream out) throws IOException {
        while ((value & ~0x7F) != 0) {
            out.writeByte((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        out.writeByte(value);
    }

    static class CountingOutputStream extends FilterOutputStream {
        private long count = 0;

        public CountingOutputStream(OutputStream out) {
            super(out);
        }

        @Override
        public void write(int b) throws IOException {
            out.write(b);
            count++;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            out.write(b, off, len);
            count += len;
        }

        public long getCount() {
            return count;
        }
    }
}
