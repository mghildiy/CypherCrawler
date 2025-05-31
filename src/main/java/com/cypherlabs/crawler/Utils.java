package com.cypherlabs.crawler;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.util.*;

public class Utils {

    static Document fetchDocument(Url url) throws IOException {
        return Jsoup.connect(url.address()).get();
    }

    static Elements extractLinks(Document doc) {
        return doc.select("a");
    }

    static List<Url> extractUrls(Set<Url> alreadyVisited, Elements links) {
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

    static void updateIndex(List<Token> tokens, Url url, Map<Token, List<Url>> tokenByDocs) {
        tokens.forEach(token -> {
            List<Url> docs = tokenByDocs.getOrDefault(token, new ArrayList<>());
            docs.add(url);
            tokenByDocs.put(token, docs);
        });
    }
}
