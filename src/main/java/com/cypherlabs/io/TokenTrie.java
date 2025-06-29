package com.cypherlabs.io;

import com.cypherlabs.crawler.Token;

import java.util.*;

public class TokenTrie {
    private TrieNode root = new TrieNode('-');

    public TrieNode createTrie(Map<Token, Long> tokenByOffSet) {
        Map<Token, Long> sortedByToken = new TreeMap<>(tokenByOffSet);
        if(sortedByToken.entrySet().size() == 0) {
            root.setTerminal(true);
        } else {
            for(Map.Entry<Token, Long> entry : sortedByToken.entrySet()) {
                insertToken(root, entry.getKey().key(), entry.getValue());
            }
        }

        return root;
    }

    private static Optional<TrieNode> getChildWithChar(TrieNode parent, char ch) {
        return parent.getChild(ch);
    }

    private static void insertToken(TrieNode parent, String str, long offset) {
        Optional<TrieNode> mayBeNode = getChildWithChar(parent, str.charAt(0));
        if(mayBeNode.isPresent()) {
            TrieNode currentNode = mayBeNode.get();
            if(str.length() == 1) {
                currentNode.setOffset(offset);
                currentNode.setTerminal(true);
                return;
            }
            insertToken(currentNode, str.substring(1), offset);
        } else {
            for(int i=0; i < str.length(); i++) {
                TrieNode node = new TrieNode(str.charAt(i));
                parent.addChild(node);
                if(i == str.length() - 1) {
                    node.setOffset(offset);
                    node.setTerminal(true);
                    return;
                }
                parent = node;
            }
        }
    }

}
