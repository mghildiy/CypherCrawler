package com.cypherlabs.io;

import com.cypherlabs.crawler.Token;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class TokenTrieTest {

    @Test
    public void testCreateTrie() {
        // prepare test data
        Map<Token, Long> tokenByOffSet = new HashMap<>();
        tokenByOffSet.put(new Token("apple"), 10L);
        tokenByOffSet.put(new Token("app"), 20L);
        tokenByOffSet.put(new Token("bat"), 11L);
        tokenByOffSet.put(new Token("batman"), 15L);
        tokenByOffSet.put(new Token("append"), 40L);
        tokenByOffSet.put(new Token("apprehend"), 100L);
        tokenByOffSet.put(new Token("appendix"), 21L);

        // create trie
        TokenTrie tokenTrie = new TokenTrie();
        TrieNode root = tokenTrie.createTrie(tokenByOffSet);

        // validate response
        assertTrue(root.getVal() == '-', "Wrong value at root");
        assertTrue(!root.isTerminal(), "root node should be terminal");
        assertEquals(2, root.getChildren().size(), "Root should have 2 children: 'a' and 'b'");

        // traverse 'a' branch
        TrieNode a = root.getChild('a').orElseThrow();
        TrieNode p1 = a.getChild('p').orElseThrow();
        TrieNode p2 = p1.getChild('p').orElseThrow();
        assertEquals(20L, p2.getOffset(), "'app' should have offset 20");
        assertTrue(p2.isTerminal(), "'app' node should be terminal");

        TrieNode l = p2.getChild('l').orElseThrow();
        TrieNode e1 = l.getChild('e').orElseThrow();
        assertEquals(10L, e1.getOffset(), "'apple' should have offset 10");
        assertTrue(e1.isTerminal(), "'apple' node should be terminal");

        TrieNode e2 = p2.getChild('e').orElseThrow();
        TrieNode n = e2.getChild('n').orElseThrow();
        TrieNode d = n.getChild('d').orElseThrow();
        assertEquals(40L, d.getOffset(), "'append' should have offset 40");
        assertTrue(d.isTerminal(), "'append' node should be terminal");

        TrieNode x = d.getChild('i').orElseThrow().getChild('x').orElseThrow();
        assertEquals(21L, x.getOffset(), "'appendix' should have offset 21");
        assertTrue(x.isTerminal(), "'appendix' node should be terminal");

        TrieNode r = p2.getChild('r').orElseThrow();
        TrieNode e3 = r.getChild('e').orElseThrow();
        TrieNode h = e3.getChild('h').orElseThrow();
        TrieNode e4 = h.getChild('e').orElseThrow();
        TrieNode n2 = e4.getChild('n').orElseThrow();
        TrieNode d2 = n2.getChild('d').orElseThrow();
        assertEquals(100L, d2.getOffset(), "'apprehend' should have offset 100");
        assertTrue(d2.isTerminal(), "'apprehend' node should be terminal");

        // Traverse 'b' branch
        TrieNode b = root.getChild('b').orElseThrow();
        TrieNode a2 = b.getChild('a').orElseThrow();
        TrieNode t = a2.getChild('t').orElseThrow();
        assertEquals(11L, t.getOffset(), "'bat' should have offset 11");
        assertTrue(t.isTerminal(), "'bat' node should be terminal");

        TrieNode m = t.getChild('m').orElseThrow();
        TrieNode a3 = m.getChild('a').orElseThrow();
        TrieNode n3 = a3.getChild('n').orElseThrow();
        assertEquals(15L, n3.getOffset(), "'batman' should have offset 15");
        assertTrue(n3.isTerminal(), "'batman' node should be terminal");
    }

    @Test
    public void testEmptyTrie() {
        TokenTrie tokenTrie = new TokenTrie();
        TrieNode root = tokenTrie.createTrie(new HashMap<>());
        assertTrue(root.getVal() == '-', "Wrong value at root");
        assertTrue(root.getChildren().isEmpty(), "Empty trie should have no children");
        assertTrue(root.isTerminal(), "root node should be terminal");
    }

    @Test
    public void testSingleToken() {
        Map<Token, Long> input = Map.of(new Token("only"), 1L);
        TokenTrie trie = new TokenTrie();
        TrieNode root = trie.createTrie(input);
        TrieNode node = root.getChild('o').get().getChild('n').get().getChild('l').get().getChild('y').get();
        assertEquals(1L, node.getOffset());
        assertTrue(node.isTerminal());
    }
}
