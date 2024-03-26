/*
 *   This file is part of the computer assignment for the
 *   Information Retrieval course at KTH.
 *
 *   Dmytro Kalpakchi, 2018
 */

package ir;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class SpellChecker {
    /** The regular inverted index to be used by the spell checker */
    Index index;

    /** K-gram index to be used by the spell checker */
    KGramIndex kgIndex;

    /** The auxiliary class for containing the value of your ranking function for a token */
    class KGramStat implements Comparable {
        double score;
        String token;

        KGramStat(String token, double score) {
            this.token = token;
            this.score = score;
        }

        public String getToken() {
            return token;
        }

        public int compareTo(Object other) {
            if (this.score == ((KGramStat)other).score) return 0;
            return this.score < ((KGramStat)other).score ? -1 : 1;
        }

        public String toString() {
            return token + ";" + score;
        }
    }

    /**
     * The threshold for Jaccard coefficient; a candidate spelling
     * correction should pass the threshold in order to be accepted
     */
    private static final double JACCARD_THRESHOLD = 0.4;


    /**
      * The threshold for edit distance for a candidate spelling
      * correction to be accepted.
      */
    private static final int MAX_EDIT_DISTANCE = 2;


    public SpellChecker(Index index, KGramIndex kgIndex) {
        this.index = index;
        this.kgIndex = kgIndex;
        System.err.println("Initialized spell checker!");
    }

    /**
     *  Computes the Jaccard coefficient for two sets A and B, where the size of set A is 
     *  <code>szA</code>, the size of set B is <code>szB</code> and the intersection 
     *  of the two sets contains <code>intersection</code> elements.
     */
    private double jaccard(int szA, int szB, int intersection) {
        //
        // YOUR CODE HERE
        //
        
        double j = (double)intersection / (double)(szA + szB - intersection);

        return j;
    }

    /**
     * Computing Levenshtein edit distance using dynamic programming.
     * Allowed operations are:
     *      => insert (cost 1)
     *      => delete (cost 1)
     *      => substitute (cost 2)
     */
    private int editDistance(String s1, String s2) {
        //
        // YOUR CODE HERE
        //
        if (s2.length() == 0)
            return s1.length();
        else if (s1.length() == 0)
            return s2.length();
        else if (s1.charAt(0) == s2.charAt(0))
            return editDistance(s1.substring(1), s2.substring(1));
        else {
            int a = editDistance(s1.substring(1), s2);
            int b = editDistance(s1, s2.substring(1));
            int c = editDistance(s1.substring(1), s2.substring(1));
            
            return myMin(1+a, 1+b, 2+c);
        }
        // int[][] dp = new int[x.length() + 1][y.length() + 1];

        // for (int i = 0; i <= x.length(); i++) {
        //     for (int j = 0; j <= y.length(); j++) {
        //         if (i == 0) {
        //             dp[i][j] = j;
        //         }
        //         else if (j == 0) {
        //             dp[i][j] = i;
        //         }
        //         else {
        //             dp[i][j] = min(dp[i - 1][j - 1] 
        //             + costOfSubstitution(x.charAt(i - 1), y.charAt(j - 1)), 
        //             dp[i - 1][j] + 1, 
        //             dp[i][j - 1] + 1);
        //         }
        //     }
        // }

        // return dp[x.length()][y.length()];
    }

    private int myMin(int a, int b, int c) {
        return Math.min(Math.min(a,b), c);
    }

    /**
     *  Checks spelling of all terms in <code>query</code> and returns up to
     *  <code>limit</code> ranked suggestions for spelling correction.
     */
    public String[] check(Query query, int limit) {
        //
        // YOUR CODE HERE
        //
        ArrayList<KGramStat> corrections = new ArrayList<KGramStat>();
        // This function is called when no results of a query is found...

        for (int i = 0; i < query.size(); i++) {
            String token = query.queryterm.get(i).term;
            Set<String> k_grams = new HashSet<String>();
            String fullToken = "^" + token + "$"; 
            for (int j = 0; j < token.length() + 3 - kgIndex.getK(); j++) { // Find all the k_grams
                String k_gram = fullToken.substring(j, j+kgIndex.getK());
                k_grams.add(k_gram);
            }

            ArrayList<String> kg_terms = kgIndex.getPostingsUnion(k_grams);
            if (kg_terms == null || kg_terms.size() == 0)
                continue;

            // kg_terms := list of all words that could be what the user intended...

            // Calculate the JC between token and all the words in kg_terms
            for (String kg_term : kg_terms) {
                ArrayList<String> r = kgIndex.getKGrams(kg_term);
                Set<String> kg_grams = null;

                if (r != null) {
                    kg_grams = new HashSet<String>(r);
                } else {
                    kg_grams = new HashSet<String>();
                    String fullkgterm = "^" + kg_term + "$"; 
                    for (int j = 0; j < kg_term.length() + 3 - kgIndex.getK(); j++) { // Find all the k_grams
                        String k_gram = fullkgterm.substring(j, j+kgIndex.getK());
                        kg_grams.add(k_gram);
                    }
                }

                //int intersect = intersectSize(kg_grams, k_grams);
                int intersect = intersectSize(r, new ArrayList<String>(k_grams));

                double jc = jaccard(kg_grams.size(), k_grams.size(), intersect);

                if (jc < JACCARD_THRESHOLD)
                    continue;
                
                // if the JC > threshold calculate the edit distance between the word w and token
                int edit = editDistance(token, kg_term);

                if (edit > MAX_EDIT_DISTANCE)
                    continue;

                // if edit distiance < threshold then w is a potential correction
                // add w to the list of corrections

                double score = calculateScore(jc, edit, kg_term);
                corrections.add(new KGramStat(kg_term, score));
            }
        }

        corrections.sort((o1, o2) -> o2.compareTo(o1));
        
        ArrayList<String> ret = new ArrayList<String>();
        for (int i = 0; i < corrections.size(); i++) {
            if (i >= limit)
                break;
            
            ret.add(corrections.get(i).token);
        }

        return ret.toArray(new String[ret.size()]);
    }

    private double calculateScore(double jc, int editdistance, String term) {
        double d = 2 * (double)index.getPostings(term).size()/(double)index.corpusSize();
        return (jc + 1/(double)editdistance)*0.5 + d;
    }

    private int intersectSize(ArrayList<String> l1, ArrayList<String> l2) {
        // 
        // YOUR CODE HERE
        //
        if (l1 == null || l2 == null || l1.size() < 1 || l2.size() < 1)
            return 0;

        if (l2.size() < l1.size()) {
            Set<String> r = new HashSet<String>(l1);
            r.addAll(l2);
            return l1.size() + l2.size() - r.size();
        } else {
            Set<String> r = new HashSet<String>(l2);
            r.addAll(l1);
            return l1.size() + l2.size() - r.size();
        }

        // l1.sort((o1, o2) -> o1.compareTo(o2));
        // l2.sort((o1, o2) -> o1.compareTo(o2));

        // int ret = 0;
        // int i = 0, j = 0;
        // while (i < l1.size() && j < l2.size()) {
        //     if (l1.get(i).equals(l2.get(j))) {
        //         ret++; // offset doesnt matter right now 
        //         i++; j++;
        //     } else if (l1.get(i).compareTo(l2.get(j)) < 0) {
        //         i++;
        //     } else {
        //         j++;
        //     }
        // }
        
        // return ret;
    }

    /**
     *  Merging ranked candidate spelling corrections for all query terms available in
     *  <code>qCorrections</code> into one final merging of query phrases. Returns up
     *  to <code>limit</code> corrected phrases.
     */
    private List<KGramStat> mergeCorrections(List<List<KGramStat>> qCorrections, int limit) {
        //
        // YOUR CODE HERE
        //
        return null;
    }
}
