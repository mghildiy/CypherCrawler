package com.cypherlabs.storage;

import com.cypherlabs.crawler.Url;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class UrlDocIdDictionary {

    private static final Logger LOGGER  = LoggerFactory.getLogger(UrlDocIdDictionary.class);

    private final Map<Url, Integer> urlByDocIdDict = new HashMap<>();
    private final List<Url> docIdToUrlList = new ArrayList<>(); // list index is docid itself

    public synchronized int addIfAbsent(Url url) {
        return urlByDocIdDict.computeIfAbsent(url, u -> {
            int docId = docIdToUrlList.size();
            docIdToUrlList.add(u);
            return docId;
        });
    }

    public Optional<Integer> getDocId(Url url) {
        return Optional.ofNullable(urlByDocIdDict.get(url));
    }

    public synchronized Optional<Url> getUrl(int docId) {
        if (docId >= 0 && docId < docIdToUrlList.size()) {
            return Optional.of(docIdToUrlList.get(docId));
        } else {
            LOGGER.info("No corresponding URL found for doc ID: {}", docId);
            return Optional.empty();
        }
    }

    public List<Url> getAllUrlsInOrder() {
        return Collections.unmodifiableList(docIdToUrlList);
    }

    public int size() {
        return docIdToUrlList.size();
    }
}
