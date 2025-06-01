package com.cypherlabs.crawler;


import org.jsoup.nodes.Document;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.cypherlabs.crawler.Utils.*;

public class Crawler {

    private static final Logger LOGGER = LoggerFactory.getLogger(Crawler.class);

    private final List<Url> seedUrls = new ArrayList<>();
    private final Map<Token, Set<Url>> tokenByDocs = new ConcurrentHashMap<>();
    private final Set<Url> alreadyVisited = ConcurrentHashMap.newKeySet();
    private final Map<Url, Integer> urlByRetryCount = new HashMap<>();
    static final int RETRY_ATTEMPTS = 3;
    private final BlockingQueue<Url> crawlFrontier = new LinkedBlockingQueue<>(10000);
    private final BlockingQueue<DocumentWithUrl> docAndUrlPairs = new LinkedBlockingQueue<>(1000);
    private final AtomicInteger activeDocumentFetchingCounter = new AtomicInteger();
    private final AtomicInteger activeDocumentProcessingCounter = new AtomicInteger();

    public static void main(String[] args) {
        Crawler crawler = new Crawler();
        crawler.crawl();
    }

    private void waitForUrlAndThenProcess() {
        Url url = null;
        Document doc = null;
        try {
            // we wait for url to be available
            url = crawlFrontier.take();
            if (alreadyVisited.contains(url)) {
                LOGGER.info("Already visited url: {}, so skipping it", url);
                return;
            }
            activeDocumentFetchingCounter.incrementAndGet();
            LOGGER.info("Starting to fetch document for url {}", url.address());
            doc = fetchDocument(url);
            LOGGER.info("Done fetching document for url {}", url.address());
            activeDocumentFetchingCounter.decrementAndGet();
            // we wait for space to be available
            docAndUrlPairs.put(new DocumentWithUrl(doc, url));
            alreadyVisited.add(url);
        } catch(IOException ioe) {
            activeDocumentFetchingCounter.decrementAndGet();
            LOGGER.error("Crawler failed to fetch document for url: {}", url.address());
            LOGGER.error(ioe.getMessage());
            urlByRetryCount.put(url, urlByRetryCount.getOrDefault(url, 0) + 1);
            if(urlByRetryCount.get(url) > RETRY_ATTEMPTS) {
                alreadyVisited.add(url);
            } else {
                updateCrawlFrontier(url);
            }
        } catch(InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOGGER.warn("Interrupted while putting document {} for url {} in queue", doc, url);
        }
    }

    private void updateCrawlFrontier(Url url) {
        try {
            crawlFrontier.put(url);
            LOGGER.info("Updated crawl frontier with url {}", url.address());
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOGGER.warn("Interrupted while putting URL in crawl frontier: {}", url);
        }
    }

    private void processDocumentIfAvailable() {
        // we don't wait
        Optional<DocumentWithUrl> mayBeDocument = Optional.ofNullable(docAndUrlPairs.poll());
        mayBeDocument.ifPresent(docAndUrlPair -> {
            activeDocumentProcessingCounter.incrementAndGet();
            Document doc = docAndUrlPair.doc();
            Url url = docAndUrlPair.url();
            LOGGER.info("Starting to process document for url {}", url.address());
            Elements links = extractLinks(doc);
            List<Url> urls = urlsNotAlreadyVisited(alreadyVisited, links);
            for (Url u : urls) {
                updateCrawlFrontier(u);
            }
            String text = extractText(doc);
            List<Token> tokens = extractTokens(text);
            // TODO: stemming
            updateIndex(tokens, url, tokenByDocs);
            activeDocumentProcessingCounter.decrementAndGet();
        });
    }

    void crawl() {
        LOGGER.info("Crawler starting to crawl....");

        seedUrls.add(new Url("https://quotes.toscrape.com/"));

        Runnable ioTaskToFetchDocument = () -> {
            while(!Thread.currentThread().isInterrupted()) {
                waitForUrlAndThenProcess();
            }
        };

        Runnable cpuTaskToProcessDocument = () -> {
            while(!Thread.currentThread().isInterrupted()) {
                processDocumentIfAvailable();
            }
        };

        crawlFrontier.addAll(seedUrls);

        // launch virtual threads to fetch documents
        ExecutorService ioExecutor = Executors.newVirtualThreadPerTaskExecutor();
        for(int i=0; i<1000; i++) {
            ioExecutor.submit(ioTaskToFetchDocument);
        }

        // launch platform threads to process documents
        // as processDocumentIfAvailable doesn't block, there is no blocking and hence
        // we keep number of platform threads within number of available cores
        ExecutorService cpuExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        for (int i = 0; i < Runtime.getRuntime().availableProcessors(); i++) {
            cpuExecutor.submit(cpuTaskToProcessDocument);
        }

        while(true) {
            boolean done = crawlFrontier.isEmpty()
                           && docAndUrlPairs.isEmpty()
                           && activeDocumentFetchingCounter.get() == 0
                           && activeDocumentProcessingCounter.get() == 0;

            if (done) break;
        }

        ioExecutor.shutdown();
        cpuExecutor.shutdown();
        try {
            if(!ioExecutor.awaitTermination(120, TimeUnit.SECONDS)) {
                ioExecutor.shutdownNow();
            }
            if(!cpuExecutor.awaitTermination(120, TimeUnit.SECONDS)) {
                cpuExecutor.shutdownNow();
            }
        } catch (InterruptedException ie) {
            LOGGER.error("Error while waiting for tasks to complete", ie);
        } finally {
            ioExecutor.close();
            cpuExecutor.close();
        }

        writeIndexToFile(tokenByDocs, LOGGER);
    }
}
