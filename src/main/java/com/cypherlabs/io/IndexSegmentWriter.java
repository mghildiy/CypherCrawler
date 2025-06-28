package com.cypherlabs.io;

import com.cypherlabs.crawler.Token;
import com.cypherlabs.crawler.Url;
import com.cypherlabs.storage.UrlDocIdDictionary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class IndexSegmentWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexSegmentWriter.class);

    /**
     * Entry point for writing a complete index segment to disk.
     *
     * This includes:
     * - Writing the postings list (postings.bin)
     * - Writing the token dictionary (both sorted and fixed-width)
     * - Writing the document table (doc_table.bin)
     *
     * @param tokenByDocs Map of tokens to the set of document IDs they appear in
     * @param urlDict Mapping between document IDs and their corresponding URLs
     * @param segmentDir Directory where all segment files will be written
     * @throws IOException If any I/O error occurs during writing
     */
    public static void writeSegment(Map<Token, Set<Integer>> tokenByDocs, UrlDocIdDictionary urlDict, Path segmentDir) throws IOException {
        Files.createDirectories(segmentDir);
        Map<Token, Long> tokenByOffSet =  writePostings(tokenByDocs, segmentDir.resolve("postings.bin"));
        writeSortedTokenDictionary(tokenByOffSet, segmentDir.resolve("token_dict.bin"));
        writeFixedWidthTokenDictionary(tokenByOffSet, segmentDir.resolve("token_dict_fixedwidth.bin"), 64);
        writeDocTable(urlDict, segmentDir.resolve("doc_table.bin"));
    }

    private static void writeBloomFilter() {

    }

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
     * @param outputFile   The path to the output binary file (e.g., postings.bin)
     * @return A map of tokens to their starting byte offset within the output file
     * @throws IOException If an I/O error occurs during writing
     */
    private static Map<Token, Long> writePostings(Map<Token, Set<Integer>> tokenByDocs, Path outputFile) throws IOException {
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

    private static void writeTokenAsTrie(Map<Token, Long> tokenByOffSet, Path outputFile) {
        TokenTrie tokenTrie = new TokenTrie();
        TrieNode trie = tokenTrie.createTrie(tokenByOffSet);
    }

    /**
     * Writes the token dictionary to token_dict.bin using UTF encoding.
     * Good fit for use cases when file size is small enough to be loaded in memory entirely.
     * - writeUTF(token string)
     * - writeLong(postings offset)
     *
     * Tokens are sorted lexicographically before writing for binary search support.
     *
     * @param tokenByOffSet Map of tokens to their offset in postings.bin
     * @param outputFile Path to the token dictionary output file
     * @throws IOException If an I/O error occurs
     */
    private static void writeSortedTokenDictionary(Map<Token, Long> tokenByOffSet, Path outputFile) throws IOException{
        Map<Token, Long> sortedByToken = new TreeMap<>(tokenByOffSet);
        try(DataOutputStream opStr = new DataOutputStream(new FileOutputStream(outputFile.toFile()))) {
            for(Map.Entry<Token, Long> entry : sortedByToken.entrySet()) {
                opStr.writeUTF(entry.getKey().key());
                opStr.writeLong(entry.getValue());
            }
        }
    }

    /**
     * Writes a fixed-width token dictionary for fast O(1) offset-based access.
     * We use it for use cases when file size is too big to be loaded in memory entirely. We use sample indexing in
     * conjunction with this file.
     *
     * Each entry:
     * - Token written as UTF-8 bytes, padded with zeros to fixedTokenWidth
     * - Followed by an 8-byte long offset
     *
     * Tokens longer than fixedTokenWidth are skipped with an error log.
     *
     * @param tokenByOffSet Map of tokens to their offset in postings.bin
     * @param outputFile Output file path
     * @param fixedTokenWidth Width in bytes for token field
     * @throws IOException If an I/O error occurs
     */
    private static void writeFixedWidthTokenDictionary(Map<Token, Long> tokenByOffSet, Path outputFile, int fixedTokenWidth) throws IOException {
        Map<Token, Long> sortedByToken = new TreeMap<>(tokenByOffSet);
        try(DataOutputStream opStr = new DataOutputStream(new FileOutputStream(outputFile.toFile()))) {
            for(Map.Entry<Token, Long> entry : sortedByToken.entrySet()) {
                String token = entry.getKey().key();
                byte[] tokenBytes = token.getBytes(StandardCharsets.UTF_8);
                if(tokenBytes.length > fixedTokenWidth) {
                    LOGGER.error("Token {} is too big. Not saving it to token dictionary", token);
                    continue;
                }
                opStr.write(tokenBytes);
                for(int i = tokenBytes.length ; i < fixedTokenWidth; i++) {
                    opStr.writeByte(0);
                }
                opStr.writeLong(entry.getValue());
            }
        }
    }

    /**
     * Writes the document table (doc_table.bin).
     *
     * Format:
     * - First writes the number of documents
     * - Then writes each document URL using writeUTF
     *
     * URLs are written in the order of their document IDs to allow
     * direct lookup by doc ID index.
     *
     * @param urlDict Document ID to URL mapping
     * @param outputFile Path to the output doc table file
     * @throws IOException If an I/O error occurs
     */
    private static void writeDocTable(UrlDocIdDictionary urlDict, Path outputFile) throws IOException {
        try(DataOutputStream opStr = new DataOutputStream(new FileOutputStream(outputFile.toFile()))) {
            List<Url> urls = urlDict.getAllUrlsInOrder();
            opStr.writeInt(urlDict.size());
            for(Url url : urls) {
                opStr.writeUTF(url.address());
            }
        }
    }
}
