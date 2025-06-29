package com.cypherlabs.io;

import com.cypherlabs.crawler.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

public class TokenDictUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(TokenDictUtils.class);

    public static void writeTokenDictionary(Map<Token, Long> tokenByOffSet, Path segmentDir) throws IOException {
        writeSortedTokenDictionary(tokenByOffSet, segmentDir.resolve("token_dict.bin"));
        writeFixedWidthTokenDictionary(tokenByOffSet, segmentDir.resolve("token_dict_fixedwidth.bin"), 64);
        TrieNode root = new TokenTrie().createTrie(tokenByOffSet);
        writeTrieToDisk(root, segmentDir.resolve("token_dict_trie.bin"));
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

    private static void writeTrieToDisk(TrieNode root, Path outputFile) throws IOException {
        try (DataOutputStream out = new DataOutputStream(new FileOutputStream(outputFile.toFile()))) {
            writeNode(root, out);
        }
    }

    private static void writeNode(TrieNode node, DataOutputStream out) throws IOException {
        // 1. Write character (root is special)
        out.writeByte(node.getVal());

        // 2. Terminal flag
        out.writeBoolean(node.isTerminal());

        // 3. Offset if terminal
        if (node.isTerminal()) {
            out.writeLong(node.getOffset());
        }

        // 4. Number of children
        Collection<TrieNode> children = node.getChildren();
        out.writeInt(children.size());

        // 5. Recurse for children
        for (TrieNode child : children) {
            writeNode(child, out);
        }
    }
}
