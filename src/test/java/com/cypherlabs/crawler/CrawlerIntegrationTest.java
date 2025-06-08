package com.cypherlabs.crawler;

import com.cypherlabs.storage.UrlDocIdDictionary;
import fi.iki.elonen.NanoHTTPD;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CrawlerIntegrationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(CrawlerIntegrationTest.class);

    static class TestFileServer extends NanoHTTPD {
        private final Path baseDir;

        public TestFileServer(int port, Path baseDir) {
            super(port);
            this.baseDir = baseDir;
        }

        @Override
        public Response serve(IHTTPSession session) {
            try {
                // Map URI to a file under baseDir
                String uri = session.getUri();
                if (uri.equals("/")) {
                    uri = "/page1.html"; // redirect root to first page
                }
                Path file = baseDir.resolve(uri.substring(1)).normalize();
                LOGGER.debug("Resolved file path to serve: {}", file);
                if (!file.startsWith(baseDir) || !Files.exists(file)) {
                    return newFixedLengthResponse(Response.Status.NOT_FOUND, "text/plain", "File Not Found");
                }
                String mime = "text/html";
                byte[] bytes = Files.readAllBytes(file);
                return newFixedLengthResponse(Response.Status.OK, mime, new ByteArrayInputStream(bytes), bytes.length);
            } catch (IOException e) {
                return newFixedLengthResponse(Response.Status.INTERNAL_ERROR, "text/plain", "Server error");
            }
        }
    }

    private static TestFileServer server;

    @BeforeAll
    static void startServer() throws IOException {
        // Start HTTP server serving from src/test/resources/test-site
        Path testSite = Paths.get("src", "test", "resources", "test-site");
        server = new TestFileServer(8080, testSite);
        server.start(NanoHTTPD.SOCKET_READ_TIMEOUT, false);
        LOGGER.info("Test HTTP server started on port 8080");
    }

    @AfterAll
    static void stopServer() {
        if (server != null) {
            server.stop();
            LOGGER.info("Test HTTP server stopped");
        }
    }

    @Test
    void testCrawlerCrawlsPagesAndIndexesTokens() {
        // prepare test data
        List<Url> testSeedUrls = List.of(new Url("http://localhost:8080/page1.html"));
        Crawler crawler = new Crawler(testSeedUrls);

        // invoke unit
        crawler.crawl();

        // validate
        Map<Token, Set<Integer>> tokenByDocs = crawler.getTokenByDocs();

        // 1) Check that some expected tokens exist
        assertTrue(tokenByDocs.containsKey(new Token("welcom")), "Token 'welcome' should be indexed");
        assertTrue(tokenByDocs.containsKey(new Token("page")), "Token 'page' should be indexed");

        // 2) Check stemming: "run" should be present but "running" should not
        assertTrue(tokenByDocs.containsKey(new Token("run")), "Token 'run' should be indexed");
        assertFalse(tokenByDocs.containsKey(new Token("running")), "Token 'running' should not be indexed");

        // 3) Check URLs linked to token "run" includes page3.html
        UrlDocIdDictionary urlDocIdDict = crawler.getUrlDocIdDict();
        Set<Url> runUrls = tokenByDocs.get(new Token("run"))
                .stream()
                .map(docId -> urlDocIdDict.getUrl(docId))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toCollection(HashSet::new));
        assertTrue(runUrls.stream().anyMatch(u -> u.address().contains("page3.html")), "Token 'run' should index page3.html");

        // 4) Optionally check that all pages are indexed (page1, page2, page3)
        Set<String> allIndexedUrls = new HashSet<>();
        tokenByDocs.values().forEach(docIdSet -> docIdSet.forEach(docId -> allIndexedUrls.add(urlDocIdDict.getUrl(docId).get().address())));
        assertTrue(allIndexedUrls.stream().anyMatch(u -> u.contains("page1.html")), "page1.html should be indexed");
        assertTrue(allIndexedUrls.stream().anyMatch(u -> u.contains("page2.html")), "page2.html should be indexed");
        assertTrue(allIndexedUrls.stream().anyMatch(u -> u.contains("page3.html")), "page3.html should be indexed");
    }

}


