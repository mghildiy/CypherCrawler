package com.cypherlabs.io;

import com.cypherlabs.crawler.Url;
import com.cypherlabs.storage.UrlDocIdDictionary;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public class DocTableUtils {

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
     * @param segmentDir Path to the output directory
     * @throws IOException If an I/O error occurs
     */
    public static void writeDocTable(UrlDocIdDictionary urlDict, Path segmentDir) throws IOException {
        try(DataOutputStream opStr =
                    new DataOutputStream(new FileOutputStream(segmentDir.resolve("doc_table.bin").toFile()))) {
            List<Url> urls = urlDict.getAllUrlsInOrder();
            opStr.writeInt(urlDict.size());
            for(Url url : urls) {
                opStr.writeUTF(url.address());
            }
        }
    }
}
