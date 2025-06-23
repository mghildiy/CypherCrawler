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

    public static void writeSegment(Map<Token, Set<Integer>> tokenByDocs, UrlDocIdDictionary urlDict, Path segmentDir) throws IOException {
        Files.createDirectories(segmentDir);
        Map<Token, Long> tokenByOffSet =  writePostings(tokenByDocs, segmentDir.resolve("postings.bin"));
        writeSortedTokenDictionary(tokenByOffSet, segmentDir.resolve("token_dict.bin"));
        writeFixedWidthTokenDictionary(tokenByOffSet, segmentDir.resolve("token_dict_fixedwidth.bin"), 64);
        writeDocTable(urlDict, segmentDir.resolve("doc_table.bin"));
    }

    private static void writeBloomFilter() {

    }

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

    private static void writeSortedTokenDictionary(Map<Token, Long> tokenByOffSet, Path outputFile) throws IOException{
        Map<Token, Long> sortedByToken = new TreeMap<>(tokenByOffSet);
        try(DataOutputStream opStr = new DataOutputStream(new FileOutputStream(outputFile.toFile()))) {
            for(Map.Entry<Token, Long> entry : sortedByToken.entrySet()) {
                opStr.writeUTF(entry.getKey().key());
                opStr.writeLong(entry.getValue());
            }
        }
    }

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
