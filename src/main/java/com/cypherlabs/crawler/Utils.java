package com.cypherlabs.crawler;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import org. slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tartarus.snowball.ext.EnglishStemmer;

public class Utils {

    private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);

    static List<Url> seedUrls() {
        String seedEnv = System.getenv("SEED_URLS");
        if (seedEnv == null || seedEnv.isBlank()) {
            throw new IllegalStateException("SEED_URLS env variable is needed, but not available.");
        }

        return Arrays.stream(seedEnv.trim().split(","))
                .map(String::trim)
                .filter(s -> !s.isBlank())
                .map(Url::new)
                .toList();
    }

    static Document fetchDocument(Url url) throws IOException {
        return Jsoup.connect(url.address())
                .userAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
                .get();
    }

    static Elements extractLinks(Document doc) {
        return doc.select("a");
    }

    static List<Url> urlsNotAlreadyVisited(Set<Url> alreadyVisited, Elements links) {
        return links.asList().stream()
                .map(link -> link.absUrl("href"))
                .filter(href -> !href.isBlank())
                .map(Url::new)
                .filter(u -> !alreadyVisited.contains(u))
                .toList();
    }

    static String extractText(Document doc) {
        return doc.body().text().toLowerCase().replaceAll("[^a-z0-9 ]", " ");
    }

    static List<Token> extractTokens(String text) {
        return Arrays.stream(text.split("\\s+"))
                .filter(t -> t.length() > 1)
                .map(Token::new)
                .toList();
    }

    static void updateIndex(List<Token> tokens, Url url, Map<Token, Set<Url>> tokenByDocs) {
        tokens.forEach(token -> tokenByDocs.computeIfAbsent(token, _ -> new HashSet<>()).add(url));
    }

    static void writeIndexToFile(Map<Token, Set<Url>> tokenByDocs, Path outputPath){
        try (BufferedWriter writer = Files.newBufferedWriter(outputPath)) {tokenByDocs.forEach((token, urls) -> {
                try {
                    writer.write("Token: " + token + "\n");
                    for (Url url : urls) {
                        writer.write("------Url: " + url + "-----\n");
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        } catch (IOException e) {
            LOGGER.error("Failed to write inverted index to file", e);
        }
    }

    public static String stem(String word) {
        EnglishStemmer stemmer = new EnglishStemmer();
        stemmer.setCurrent(word);
        if (stemmer.stem()) {
            return stemmer.getCurrent();
        }

        return word;
    }
}
