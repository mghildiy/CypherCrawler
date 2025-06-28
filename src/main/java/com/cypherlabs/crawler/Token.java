package com.cypherlabs.crawler;

public record Token(String key) implements Comparable<Token> {

    @Override
    public int compareTo(Token other) {
        return this.key.compareTo(other.key);
    }
}
