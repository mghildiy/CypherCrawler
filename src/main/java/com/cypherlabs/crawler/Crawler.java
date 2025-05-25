package com.cypherlabs.crawler;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Crawler {

    private static Logger LOGGER = LoggerFactory.getLogger(Crawler.class);

    static List<Url> seedUrls = new ArrayList<>();
    static Queue<Url> crawlFrontier = new LinkedList<>();

    public static void main(String[] args) {
        crawl();
    }

    static void processUrl(Url url) {
        try {
            Document htmlDoc = Jsoup.connect(url.address())
                    .get();

            // TODO fetch html document, parse it to generate tokens, extract urls and put in crawlFrontier(depth first)
            // TODO persist token and url after stemming etc
        } catch (IOException ioe) {
            LOGGER.error("Crawler failed to fetch url: {}", url.address());
            ioe.printStackTrace();
            crawlFrontier.add(url);
        } catch(IllegalStateException ise) {

        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    static void crawl() {
        LOGGER.info("Crawler starting to crawl....");

        crawlFrontier.addAll(seedUrls);
        while(!crawlFrontier.isEmpty()) {
            Optional<Url> toVisit = Optional.ofNullable(crawlFrontier.poll());
            toVisit.ifPresent(Crawler::processUrl);
        }
    }
}
