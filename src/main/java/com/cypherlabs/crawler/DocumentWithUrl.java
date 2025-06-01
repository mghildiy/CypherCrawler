package com.cypherlabs.crawler;

import org.jsoup.nodes.Document;

public record DocumentWithUrl(Document doc, Url url) {
}
