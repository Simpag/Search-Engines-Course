/*
 *   This file is part of the computer assignment for the
 *   Information Retrieval course at KTH.
 *
 *   Dmytro Kalpakchi, 2018
 */

package ir;

import java.io.*;
import java.util.*;
import java.nio.charset.StandardCharsets;


public class KGramIndex {

    /** Mapping from term ids to actual term strings */
    HashMap<Integer,String> id2term = new HashMap<Integer,String>();

    /** Mapping from term strings to term ids */
    HashMap<String,Integer> term2id = new HashMap<String,Integer>();

    /** Index from k-grams to list of term ids that contain the k-gram */
    HashMap<String,List<KGramPostingsEntry>> index = new HashMap<String,List<KGramPostingsEntry>>();

    /** Index from word to all k-grams */
    HashMap<String, ArrayList<String>> word2kgrams = new HashMap<String,ArrayList<String>>();

    /** The ID of the last processed term */
    int lastTermID = -1;

    /** Number of symbols to form a K-gram */
    int K = 3;

    public KGramIndex(int k) {
        K = k;
        if (k <= 0) {
            System.err.println("The K-gram index can't be constructed for a negative K value");
            System.exit(1);
        }
    }

    /** Generate the ID for an unknown term */
    private int generateTermID() {
        return ++lastTermID;
    }

    public int getK() {
        return K;
    }

    public ArrayList<String> getKGrams(String word) {
        if (!word2kgrams.containsKey(word))
            return null;
        
        return word2kgrams.get(word);
    }

    /**
     *  Get intersection of two postings lists
     */
    private List<KGramPostingsEntry> intersect(List<KGramPostingsEntry> p1, List<KGramPostingsEntry> p2) {
        // 
        // YOUR CODE HERE
        //
        if (p1 == null || p2 == null)
            return null;

        List<KGramPostingsEntry> ret = new ArrayList<KGramPostingsEntry>();
        int i = 0, j = 0;
        while (i < p1.size() && j < p2.size()) {
            if (p1.get(i).tokenID == p2.get(j).tokenID) {
                ret.add(p1.get(i)); // offset doesnt matter right now 
                i++; j++;
            } else if (p1.get(i).tokenID < p2.get(j).tokenID) {
                i++;
            } else {
                j++;
            }
        }

        if (ret.size() > 0) {
            return ret;
        }
        
        return null;
    }

      /**
     *  Get intersection of two postings lists
     */
    private List<KGramPostingsEntry> union(List<KGramPostingsEntry> p1, List<KGramPostingsEntry> p2) {
        // 
        // YOUR CODE HERE
        //
        if (p1 == null && p2 == null)
            return null;

        if (p1 != null && p2 == null)
            return p1;

        if (p1 == null && p2 != null)
            return p2;

        List<KGramPostingsEntry> ret = new ArrayList<KGramPostingsEntry>();
        int i = 0, j = 0;
        while (i < p1.size() && j < p2.size()) {
            if (p1.get(i).tokenID == p2.get(j).tokenID) {
                ret.add(p1.get(i));
                i++; j++;
            } else if (p1.get(i).tokenID < p2.get(j).tokenID) {
                ret.add(p1.get(i));
                i++;
            } else {
                ret.add(p2.get(j));
                j++;
            }
        }

        while (i < p1.size()) {
            ret.add(p1.get(i++));
        }

        while (j < p2.size()) {
            ret.add(p2.get(j++));
        }

        if (ret.size() > 0) {
            return ret;
        }
        
        return null;
    }


    /** Inserts all k-grams from a token into the index. */
    public void insert( String token ) {
        //
        // YOUR CODE HERE
        //

        if (term2id.containsKey(token))
            return;

        String fullToken = "^" + token.strip() + "$";
        int id = generateTermID();
        HashMap<String, Boolean> k_grams_used = new HashMap<String, Boolean>();

        id2term.put(id, token);
        term2id.put(token, id);
        
        KGramPostingsEntry entry = new KGramPostingsEntry(id);
        for (int i = 0; i < token.length() + 3 - K; i++) {
            String k_gram = fullToken.substring(i, i+K);

            if (k_grams_used.containsKey(k_gram))
                continue;
            else
                k_grams_used.put(k_gram, true);
            
            if (index.containsKey(k_gram)) {
                List<KGramPostingsEntry> e = index.get(k_gram);
                e.add(entry);
                index.put(k_gram, e);
            } else {
                List<KGramPostingsEntry> e = new ArrayList<KGramPostingsEntry>();
                e.add(entry);
                index.put(k_gram, e);
            }
        }

        word2kgrams.put(token, new ArrayList<String>(k_grams_used.keySet()));
    }

    /** Get postings for the given k-gram */
    public List<KGramPostingsEntry> getPostings(String kgram) {
        //
        // YOUR CODE HERE
        //
        return index.get(kgram);
    }

    /** Get id of a term */
    public Integer getIDByTerm(String term) {
        return term2id.get(term);
    }

    /** Get a term by the given id */
    public String getTermByID(Integer id) {
        return id2term.get(id);
    }

    /** Get the union of multiple k-grams. Returns a list of terms */
    public ArrayList<String> getPostingsUnion(Set<String> k_grams) {
        ArrayList<String> ret = new ArrayList<String>();
        List<KGramPostingsEntry> postings = null;
        for (String kgram : k_grams) {
            if (kgram.length() != K) {
                System.err.println("Cannot search k-gram index: " + kgram.length() + "-gram provided instead of " + K + "-gram");
                System.exit(1);
            }

            if (postings == null) {
                postings = getPostings(kgram);
            } else {
                postings = union(postings, getPostings(kgram));
            }
        }

        if (postings == null) {
            //System.err.println("Found 0 posting(s)");
            return null;
        } else {
            int resNum = postings.size();
            //System.err.println("Found " + resNum + " posting(s)");
            
            for (int i = 0; i < resNum; i++) {
                ret.add(getTermByID(postings.get(i).tokenID));
            }

            return ret;
        }
    }

    /** Get the intersection of multiple k-grams. Returns a list of terms */
    public List<String> getPostingsIntersection(String[] k_grams) {
        List<String> ret = new ArrayList<String>();
        List<KGramPostingsEntry> postings = null;
        for (String kgram : k_grams) {
            if (kgram.length() != K) {
                System.err.println("Cannot search k-gram index: " + kgram.length() + "-gram provided instead of " + K + "-gram");
                System.exit(1);
            }

            if (postings == null) {
                postings = getPostings(kgram);
            } else {
                postings = intersect(postings, getPostings(kgram));
            }
        }

        if (postings == null) {
            //System.err.println("Found 0 posting(s)");
            return null;
        } else {
            int resNum = postings.size();
            //System.err.println("Found " + resNum + " posting(s)");
            
            for (int i = 0; i < resNum; i++) {
                ret.add(getTermByID(postings.get(i).tokenID));
            }

            return ret;
        }
    }

    public List<String> getPostingsIntersection(ArrayList<String> k_grams) {
        String[] r = k_grams.toArray(new String[k_grams.size()]);
        return getPostingsIntersection(r);
    }

    private static HashMap<String,String> decodeArgs( String[] args ) {
        HashMap<String,String> decodedArgs = new HashMap<String,String>();
        int i=0, j=0;
        while ( i < args.length ) {
            if ( "-p".equals( args[i] )) {
                i++;
                if ( i < args.length ) {
                    decodedArgs.put("patterns_file", args[i++]);
                }
            } else if ( "-f".equals( args[i] )) {
                i++;
                if ( i < args.length ) {
                    decodedArgs.put("file", args[i++]);
                }
            } else if ( "-k".equals( args[i] )) {
                i++;
                if ( i < args.length ) {
                    decodedArgs.put("k", args[i++]);
                }
            } else if ( "-kg".equals( args[i] )) {
                i++;
                if ( i < args.length ) {
                    decodedArgs.put("kgram", args[i++]);
                }
            } else {
                System.err.println( "Unknown option: " + args[i] );
                break;
            }
        }
        return decodedArgs;
    }

    public static void main(String[] arguments) throws FileNotFoundException, IOException {
        HashMap<String,String> args = decodeArgs(arguments);

        int k = Integer.parseInt(args.getOrDefault("k", "3"));
        KGramIndex kgIndex = new KGramIndex(k);

        File f = new File(args.get("file"));
        Reader reader = new InputStreamReader( new FileInputStream(f), StandardCharsets.UTF_8 );
        Tokenizer tok = new Tokenizer( reader, true, false, true, args.get("patterns_file") );
        while ( tok.hasMoreTokens() ) {
            String token = tok.nextToken();
            kgIndex.insert(token);
        }

        String[] kgrams = args.get("kgram").split(" ");
        List<KGramPostingsEntry> postings = null;
        for (String kgram : kgrams) {
            if (kgram.length() != k) {
                System.err.println("Cannot search k-gram index: " + kgram.length() + "-gram provided instead of " + k + "-gram");
                System.exit(1);
            }

            if (postings == null) {
                postings = kgIndex.getPostings(kgram);
            } else {
                postings = kgIndex.intersect(postings, kgIndex.getPostings(kgram));
            }
        }
        if (postings == null) {
            System.err.println("Found 0 posting(s)");
        } else {
            int resNum = postings.size();
            System.err.println("Found " + resNum + " posting(s)");
            if (resNum > 10) {
                System.err.println("The first 10 of them are:");
                resNum = 10;
            }
            for (int i = 0; i < resNum; i++) {
                System.err.println(kgIndex.getTermByID(postings.get(i).tokenID) + "; " + postings.get(i).tokenID);
            }
        }
    }
}
