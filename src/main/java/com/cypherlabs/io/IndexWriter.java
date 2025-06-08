package com.cypherlabs.io;

import com.cypherlabs.crawler.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

public class IndexWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger(IndexWriter.class);

    // Magic number for "Cyph"
    private static final int MAGIC_NUMBER = 0x43797068;

    public static void writeIndexToBinaryFile(Map<Token, Set<Integer>> invertedIndex, Path outputPath) throws IOException {
        try (DataOutputStream out = new DataOutputStream(new FileOutputStream(outputPath.toFile()))) {

            // Write magic number
            out.writeInt(MAGIC_NUMBER);

            // Write number of tokens
            out.writeInt(invertedIndex.size());

            for (Map.Entry<Token, Set<Integer>> entry : invertedIndex.entrySet()) {
                Token token = entry.getKey();
                Set<Integer> docIds = entry.getValue();

                // Write token(it would first write length of string, adn then actual string)
                out.writeUTF(token.key());

                // Write number of docs ids
                out.writeInt(docIds.size());

                // Write each URL
                for (Integer docId : docIds) {
                    out.writeInt(docId);
                }
            }
        }
    }

    public static void writeIndexToTextFile(Map<Token, Set<Integer>> invertedIndex, Path outputPath) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(outputPath)) {
            for(Map.Entry<Token, Set<Integer>> entry : invertedIndex.entrySet()) {
                writer.write("Token: " + entry.getKey() + "\n");
                for (Integer docId : entry.getValue()) {
                    writer.write("------DocId: " + docId + "-----\n");
                }
            }
        }
    }
}

