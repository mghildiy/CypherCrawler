package com.cypherlabs.crawler;

import org.jsoup.nodes.Document;

import java.io.IOException;
import java.util.*;

import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.cypherlabs.crawler.Utils.*;

public class Crawler {

    private static final Logger LOGGER = LoggerFactory.getLogger(Crawler.class);

    static List<Url> seedUrls = new ArrayList<>();
    static Queue<Url> crawlFrontier = new LinkedList<>();
    static Map<Token, List<Url>> tokenByDocs = new HashMap<>();
    static Set<Url> alreadyVisited = new HashSet<>();
    static Map<Url, Integer> urlByRetryCount = new HashMap<>();
    static final int RETRY_ATTEMPTS = 3;

    public static void main(String[] args) {
        Crawler crawler = new Crawler();
        crawler.crawl();
    }

    void process(Url url) throws IOException {
        Document doc = fetchDocument(url);
        Elements links = extractLinks(doc);
        List<Url> urls = extractUrls(alreadyVisited, links);
        crawlFrontier.addAll(urls);
        String text = extractText(doc);
        List<Token> tokens = extractTokens(text);
        // stemming, process tokens
        updateIndex(tokens, url, tokenByDocs);
    }

    void processUrl(Url url) {
        if(alreadyVisited.contains(url)) {
            LOGGER.info("Already visited url: {}, so skipping it", url);

            return;
        }

        try {
            process(url);
            alreadyVisited.add(url);
        } catch (IOException ioe) {
            LOGGER.error("Crawler failed to fetch document for url: {}", url.address());
            LOGGER.error(ioe.getMessage());
            urlByRetryCount.put(url, urlByRetryCount.getOrDefault(url, 0) + 1);
            if(urlByRetryCount.get(url) > RETRY_ATTEMPTS) {
                alreadyVisited.add(url);
            } else {
                crawlFrontier.add(url);
            }
        }
    }

    void crawl() {
        LOGGER.info("Crawler starting to crawl....");

        crawlFrontier.addAll(seedUrls);
        while(!crawlFrontier.isEmpty()) {
            Optional<Url> toVisit = Optional.ofNullable(crawlFrontier.poll());
            toVisit.ifPresent(this::processUrl);
        }
    }
}
