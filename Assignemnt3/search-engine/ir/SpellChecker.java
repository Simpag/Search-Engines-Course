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
    private static final double JACCARD_THRESHOLD = 0.35;


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
        // if (s2.length() == 0)
        //     return s1.length();
        // else if (s1.length() == 0)
        //     return s2.length();
        // else if (s1.charAt(0) == s2.charAt(0))
        //     return editDistance(s1.substring(1), s2.substring(1));
        // else {
        //     int a = editDistance(s1.substring(1), s2);
        //     int b = editDistance(s1, s2.substring(1));
        //     int c = editDistance(s1.substring(1), s2.substring(1));
            
        //     return myMin(1+a, 1+b, 2+c);
        // }
        int[][] d = new int[s1.length() + 1][s2.length() + 1];

        for (int i = 1; i < s1.length()+1; i++)
            d[i][0] = i;

        for (int i = 1; i < s2.length()+1; i++)
            d[0][i] = i;

        for (int i = 0; i < s1.length(); i++) {
            for (int j = 0; j < s2.length(); j++) {
                int subCost = 2;
                if (s1.charAt(i) == s2.charAt(j))
                    subCost = 0;

                d[i+1][j+1] = myMin(d[i][j+1] + 1,
                                    d[i+1][j] + 1,
                                    d[i][j] + subCost);
            }
        }

        return d[s1.length()][s2.length()];
    }

    private int myMin(int a, int b, int c) {
        return Math.min(Math.min(a,b), c);
    }

    /**
     *  Checks spelling of all terms in <code>query</code> and returns up to
     *  <code>limit</code> ranked suggestions for spelling correction.
     */
    public String[] check(Query query, int limit, QueryType qt) {
        //
        // YOUR CODE HERE
        //
        // This function is called when no results of a query is found...
        ArrayList<ArrayList<KGramStat>> query_corrections = new ArrayList<ArrayList<KGramStat>>();
        for (int i = 0; i < query.size(); i++) {
            String token = query.queryterm.get(i).term;
            // If the query returns results, skip it
            PostingsList p = index.getPostings(token);
            if (p != null && p.size() > 0) {
                ArrayList<KGramStat> c = new ArrayList<KGramStat>();
                c.add(new KGramStat(token, 1_000_000));
                query_corrections.add(c);
                continue;
            }

            ArrayList<KGramStat> corrections = new ArrayList<KGramStat>();

            // Gets the k_grams and the full terms corresponding to these
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
            double maxScore = -1; // used to normalize for each query term
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
                if (score > maxScore)
                    maxScore = score;
                
                corrections.add(new KGramStat(kg_term, score));
            }

            corrections.sort((o1, o2) -> o2.compareTo(o1));

            // Only grab the top "limit" tokens
            // ArrayList<KGramStat> c = new ArrayList<KGramStat>();
            // for (int j = 0; j < corrections.size(); j++) {
            //     if (j >= limit)
            //         break;
                
            //     //corrections.get(j).score /= maxScore; // Normalize the scores
            //     c.add(corrections.get(j));
            // }
            // query_corrections.add(c);
            query_corrections.add(corrections);
        }

        ArrayList<KGramStat> merged = mergeCorrections(query_corrections, limit, qt);

        if (merged == null || merged.size() == 0)
            return null;

        merged.sort((o1, o2) -> o2.compareTo(o1)); // idk if this should be done
        
        ArrayList<String> ret = new ArrayList<String>();
        for (int i = 0; i < merged.size(); i++) {
            if (i >= limit)
                break;
            
            ret.add(merged.get(i).getToken());
        }

        return ret.toArray(new String[ret.size()]);
    }

    private double calculateScore(double jc, int editdistance, String term) {
        // All values are between 0 and 1
        PostingsList p = index.getPostings(term);
        double d = 0;
        if (p != null)
            d = 4.0 * (double)p.size()/(double)index.corpusSize();
        
        d += jc;

        if (editdistance > 0)
            d += 1.0/(double)editdistance;
        else
            d += 1.0;

        return d;
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
    }

    /**
     *  Merging ranked candidate spelling corrections for all query terms available in
     *  <code>qCorrections</code> into one final merging of query phrases. Returns up
     *  to <code>limit</code> corrected phrases.
     */
    private ArrayList<KGramStat> mergeCorrections(ArrayList<ArrayList<KGramStat>> qCorrections, int limit, QueryType qt) {
        /*
         * Find combinations with the highest sum of scores for the entire query
         * Not optimal but a fast greedy approach
         * qCorrections are sorted in ascending score
         * Take highest score and form a query, then remove the term with lowest score
        */
        ArrayList<KGramStat> ret = new ArrayList<KGramStat>();

        if (qCorrections == null || qCorrections.size() <= 0)
            return null;

        int[] indicies = new int[qCorrections.size()];

        int maxCombinations = 1;
        for (int i = 0; i < qCorrections.size(); i++) {
            maxCombinations *= qCorrections.get(i).size();
        }

        for (int q = 0; q < limit; q++) {
            String token = "";
            double score = 0;
            double[] scores = new double[qCorrections.size()];
            double lowestScore = Double.MAX_VALUE;
            int lowestScoreIdx = -1;
            for (int i = 0; i < qCorrections.size(); i++) {
                if (qCorrections.get(i).size() < 1)
                    continue;

                token += qCorrections.get(i).get(indicies[i]).getToken() + " ";
                double s = qCorrections.get(i).get(indicies[i]).score;
                scores[i] = s;
                score += s;
                if (s < lowestScore) {
                    lowestScore = s;
                    lowestScoreIdx = i;
                }
            }

            if (lowestScoreIdx < 0)
                continue;

            indicies[lowestScoreIdx]++;

            // If we have used all of the tokens for the query term,
            // then reset the count
            if (indicies[lowestScoreIdx] >= qCorrections.get(lowestScoreIdx).size()) {
                indicies[lowestScoreIdx] = 0;
                double nextLowest = Double.MAX_VALUE;
                int nextLowestIdx = -1;

                for (int i = 0; i < scores.length; i++) {
                    if (i == lowestScoreIdx)
                        continue;

                    if (scores[i] < nextLowest) {
                        nextLowest = scores[i];
                        nextLowestIdx = i;
                    }
                }

                if (nextLowestIdx >= 0) {
                    indicies[nextLowestIdx]++;
                    if (indicies[nextLowestIdx] >= qCorrections.get(nextLowestIdx).size())
                        indicies[nextLowestIdx] = 0;
                }   
            }

            // Add the new "best" query
            ret.add(new KGramStat(token, score));

            if (ret.size() >= maxCombinations)
                break;
        }

        return ret;
    }
}
