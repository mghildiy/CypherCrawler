package com.cypherlabs.io;

import java.util.*;

public class TrieNode {

    private char val;
    boolean isTerminal;
    long offset = -1;
    Map<Character, TrieNode> children = new HashMap<>();


    public TrieNode(char val) {
        this.val = val;
    }

    public char getVal() {
        return val;
    }

    public boolean isTerminal() {
        return isTerminal;
    }

    public void setTerminal(boolean terminal) {
        isTerminal = terminal;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public Collection<TrieNode> getChildren() {
        return children.values();
    }

    public void addChild(TrieNode child) {
        this.children.put(child.getVal(), child);
    }

    public Optional<TrieNode> getChild(char ch) {
        return Optional.ofNullable(children.get(ch));
    }
}
