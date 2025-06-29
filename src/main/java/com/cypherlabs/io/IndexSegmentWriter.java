package com.cypherlabs.io;

import com.cypherlabs.crawler.Token;
import com.cypherlabs.crawler.Url;
import com.cypherlabs.storage.UrlDocIdDictionary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
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
     * @param compact Flag for writing compactly using (delta+var int) or with fixed byte length
     * @throws IOException If any I/O error occurs during writing
     */
    public static void writeSegment(Map<Token, Set<Integer>> tokenByDocs, UrlDocIdDictionary urlDict, Path segmentDir, boolean compact) throws IOException {
        Files.createDirectories(segmentDir);
        Map<Token, Long> tokenByOffSet =  PostingsUtils.writePostings(tokenByDocs, segmentDir, compact);
        TokenDictUtils.writeTokenDictionary(tokenByOffSet, segmentDir);
        DocTableUtils.writeDocTable(urlDict, segmentDir);
    }
}
