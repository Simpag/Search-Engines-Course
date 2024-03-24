/*  
 *   This file is part of the computer assignment for the
 *   Information Retrieval course at KTH.
 * 
 *   Johan Boye, 2017
 */  

package ir;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.stream.Collectors;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.Math;

/**
 *  Searches an index for results of a query.
 */
public class Searcher {

    /** The index to be searched by this Searcher. */
    Index index;

    /** The k-gram index to be searched by this Searcher */
    KGramIndex kgIndex;

    HITSRanker hitsRanker;
    
    /** Constructor */
    public Searcher( Index index, KGramIndex kgIndex ) {
        this.index = index;
        this.kgIndex = kgIndex;
        this.hitsRanker = new HITSRanker("C:/Users/Simon/Documents/Github/Search-Engines-Course/Assignemnt2/PageRanking/pagerank/linksDavis.txt", 
        "C:/Users/Simon/Documents/Github/Search-Engines-Course/Assignemnt2/PageRanking/pagerank/davisTitles.txt", index);
    }

    /**
     *  Searches the index for postings matching the query.
     *  @return A postings list representing the result of the query.
     */
    public PostingsList search( Query query, QueryType queryType, RankingType rankingType, NormalizationType normType, double ratio) { 
        //
        //  REPLACE THE STATEMENT BELOW WITH YOUR CODE
        // 
        PostingsList ret = null;
        //ArrayList<PostingsList> posts = new ArrayList<PostingsList>();
        ArrayList<Query> queries = createWildcardQueries(query);
        //ArrayList<Query> queries = new ArrayList<>();
        //queries.add(query);

        // zombie should return:::
        // zombie;1.0, emilymaas.jpg;0.8, forgive;0.8, wiki;0.8, for;1.6, promise;0.8, while;0.8, good;0.8, rolling;0.8, 2006;0.8, her;0.8, too;0.8, children;0.8, and;0.8, edits;0.8, katamari;0.8, http://kawaiikitcat.livejournal.com/profile;0.8, up;0.8, pigeon;0.8, a;2.4000000000000004, image;0.8, though;0.8, in;0.8, was;0.8, i;0.8, is;0.8, also;0.8, livejournal;0.8, girl;1.6, molesting;0.8, any;0.8, really;0.8, nice;0.8, odd;0.8, she;2.4000000000000004, weird;0.8, s;0.8, at;0.8, obsesses;0.8, much;0.8, who;0.8,


        // returns:
        // zombie;1.0, emilymaas.jpg;0.8, forgive;0.8, wiki;0.8, for;1.6, promise;0.8, while;0.8, good;0.8, rolling;0.8, 2006;0.8, her;0.8, too;0.8, children;0.8, and;0.8, edits;0.8, katamari;0.8, http://kawaiikitcat.livejournal.com/profile;0.8, up;0.8, pigeon;0.8, a;2.4000000000000004, image;0.8, though;0.8, in;0.8, was;0.8, i;0.8, is;0.8, also;0.8, livejournal;0.8, girl;1.6, molesting;0.8, any;0.8, really;0.8, nice;0.8, odd;0.8, she;2.4000000000000004, weird;0.8, s;0.8, at;0.8, obsesses;0.8, much;0.8, who;0.8,

        for (Query q : queries) {
            PostingsList p = new PostingsList();

            // for (int i = 0; i < q.size(); i++) {
            //     System.err.print(q.queryterm.get(i).term + ";" + q.queryterm.get(i).weight  + ", ");
            // }
            // System.err.println("\n");
    
            switch (queryType) {
                case INTERSECTION_QUERY:
                    p = intersection_query(q);
                    break;
    
                case PHRASE_QUERY:
                    p = phrase_query(q);
                    break;
    
                case RANKED_QUERY:
                    p = ranked_query(q, rankingType, normType, ratio);
                    break;
            
                default:
                    System.err.println("Some error occured in Searcher.java (wrong QueryType)");
                    break;
            }
            
            if (p == null) {
                continue;
            }

            //posts.add(p);
            ret = merge_postingslists(ret, p);      
        }

         

        if (queryType == QueryType.RANKED_QUERY) {
            ret.sortByScores();
        }
        
        return ret;
    }

    /*
     * Wildcard queries:
     * 
     */

    private ArrayList<Query> createWildcardQueries(Query query) {
        ArrayList<Query> queries = new ArrayList<Query>();
        ArrayList<ArrayList<String>> wildcards = new ArrayList<ArrayList<String>>();
        HashMap<String, Double> wildcardWeights = new HashMap<String, Double>();

        // System.err.println("Number of terms in query: " + query.size());
        // for (int i = 0; i < query.size(); i++) {
        //     System.err.print(query.queryterm.get(i).term + ";" + query.queryterm.get(i).weight  + ", ");
        // }
        // System.err.println("\n");

        for (int i = 0; i < query.size(); i++) {
            Set<String> terms = new HashSet<String>();
            String token = query.queryterm.get(i).term;
            int idx = token.indexOf("*");

            if (idx < 0) {
                if (wildcardWeights.containsKey(token)) {
                    double w = wildcardWeights.get(token) + query.queryterm.get(i).weight;
                    wildcardWeights.put(token, w);
                } else {
                    terms.add(token);
                    wildcards.add(new ArrayList<String>(terms));
                    wildcardWeights.put(token, query.queryterm.get(i).weight);
                }
                continue;
            }

            String fullToken = "^" + token.substring(0, idx) + token.substring(idx)  + "$"; // Might not work because of .
            Set<String> k_grams = new HashSet<String>();
            for (int j = 0; j < token.length() + 3 - kgIndex.getK(); j++) { // Find all the k_grams
                String k_gram = fullToken.substring(j, j+kgIndex.getK());

                if (k_gram.indexOf("*") < 0)
                    k_grams.add(k_gram);
            }

            List<String> kg_terms = kgIndex.getPostingsIntersection(k_grams.toArray(new String[k_grams.size()])); // Get the intersection of all terms using the k_grams
            if (kg_terms == null || kg_terms.size() == 0)
                continue;

            String regex_token = "^" + token.substring(0, idx) + "." + token.substring(idx)  + "$";
            for (String term : kg_terms) {
                if (term.matches(regex_token)) {
                    terms.add(term);
                    if (wildcardWeights.containsKey(term)) {
                        double w = wildcardWeights.get(term) + query.queryterm.get(i).weight;
                        wildcardWeights.put(term, w);
                    } else {
                        wildcardWeights.put(term, query.queryterm.get(i).weight);
                    }
                }
            }
            if (terms.size() == 0)
                continue;
                
            wildcards.add(new ArrayList<String>(terms));
        }

        // Construct all the queries

        /* mo*y transfer
         * 
         * 0 : money, monkey, moosey
         * 1 : transfer
         */

         /* b* colo*r
         * 
         * 0 : be, been, bat
         * 1 : color, colour
         */
        ArrayList<ArrayList<String>> combinations = new ArrayList<>();
        combine(wildcards, new ArrayList<>(), combinations, 0);
        
        for (ArrayList<String> qs : combinations) {
            Query q = new Query();
            for (String term : qs) {
                //Query.QueryTerm qt = query.getQueryTerm(term);
                q.appendTerm(term, wildcardWeights.get(term));
            }
            queries.add(q);
        }

        /*for (ArrayList<String> q : combinations) {
            String s = "";
            for (String term : q) {
                s += term + " ";
            }
            queries.add(new Query(s));
        }*/

        return queries;
    }

    
    private static void combine(ArrayList<ArrayList<String>> listOfLists, ArrayList<String> current, ArrayList<ArrayList<String>> combinations, int index) {
        if (current.size() == listOfLists.size()) { // Check if current combination has the desired length
            combinations.add(new ArrayList<>(current)); // Add a copy to avoid modification
            return;
        }

        if (index == listOfLists.size()) {
            return; // Don't proceed if index reaches the end before reaching desired length
        }

        for (String element : listOfLists.get(index)) {
            current.add(element);
            combine(listOfLists, current, combinations, index + 1);
            current.remove(current.size() - 1); // Backtrack by removing the element
        }
    }

    private PostingsList ranked_query(Query query, RankingType rankingType, NormalizationType normType, double ratio) {
        ArrayList<PostingsList> terms = new ArrayList<PostingsList>();
        for (int i = 0, size = query.size(); i < size; i++)
        {
            String token = query.queryterm.get(i).term;
            PostingsList p = index.getPostings(token);
            if (p == null) 
                continue;
            terms.add(p);
        }

        switch (rankingType) {
            case TF_IDF:
                terms = calculate_tf_idf(query, normType);       
                break;

            case PAGERANK:
                calculate_page_ranking(terms);       
                break;

            case COMBINATION:
                calculate_combined_ranking(terms, normType, ratio);       
                break;

            case HITS:
                calculate_hits_ranking(terms, ratio);
                break;
        
            default:
                System.err.println("Some error occured in Searcher.java (wrong RankingType)");
                break;
        }

        PostingsList res = merge_postingslists(terms);

        // for (int i = 0; i < 50; i++) {
        //     System.err.println("1 " + getFileName(Index.docNames.get(res.get(i).docID)));
        // }

        //nDCG(50, res);

        return res;
    }

    private PostingsList merge_postingslists(ArrayList<PostingsList> lists) {
        long startTime = System.currentTimeMillis();

        if (lists == null)
            return null;

        // merge
        Iterator<PostingsList> piter = lists.iterator();
        PostingsList res;
        if (piter.hasNext())
            res = piter.next();
        else
            return null;

        while (piter.hasNext()) {
            Iterator<PostingsEntry> eiter = piter.next().iterator();
            PostingsEntry e;
            while (eiter.hasNext()) {
                e = eiter.next();
                if (res.containsDocID(e.docID)) {
                    res.addScore(e.docID, e.score);
                } else {
                    res.insert(e.docID, e.score, 0);
                    //res.sortByDocID();
                }
            }
        }

        /*PostingsList res = new PostingsList();
        for (PostingsList p : terms) {
            for (int j = 0; j < p.size(); j++) {
                if (res.containsDocID(p.get(j).docID)) {
                    res.addScore(p.get(j).docID, p.get(j).score);
                } else {
                    res.insert(p.get(j).docID, p.get(j).score, 0);
                }
            }
        }*/

        // long elapsedTime = System.currentTimeMillis() - startTime;
        // System.err.println("Merge took: " + elapsedTime + " ms");

        return res;
    }

    private void nDCG(int k, PostingsList list) {
        if (list.size() < k) {
            System.err.println("Number of returned documents is less than k=" + k +", retrieved: " + list.size() + " documents");
            return;
        }

        HashMap<String, Integer> ratings = new HashMap<String, Integer>();
        ArrayList<Integer> sortedRatings = new ArrayList<Integer>();
        File f = new File("assignment3/average_relevance_filtered.txt");
        
        if (!f.exists()) {
            System.err.println("average_relevance_filtered.txt file!");
            return;
        }

        try (BufferedReader br = new BufferedReader(new FileReader(f))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] data = line.strip().split(" ");
                ratings.put(data[0], Integer.parseInt(data[1]));
                sortedRatings.add(Integer.parseInt(data[1]));
            }
        } catch ( IOException e ) {
            e.printStackTrace();
        }
        sortedRatings.sort((o1, o2) -> Integer.compare(o2, o1));

        String missingDocs = "";
        double DCG = 0.0;
        double iDCG = 0.0;
        for (int i = 0; i < k; i++) {
            String docName = getFileName(Index.docNames.get(list.get(i).docID));

            if (docName.equals("Mathematics.f")) {
                System.err.println("Skipping: " + docName);
                continue;
            }

            assert ratings.containsKey(docName) : "Ratings does not cotain document: " + docName;
            if (!ratings.containsKey(docName)) {
                missingDocs += docName + ", ";
                continue;
            }
            double rel_i = ratings.get(docName);
            double denom = Math.log(i+2) / Math.log(2.0);
            DCG += rel_i / denom;

            rel_i = sortedRatings.get(i);
            iDCG += rel_i / denom;
        }

        double nDCG = DCG / iDCG;

        if (!missingDocs.equals(""))
            System.err.println("No ratings exsists for documents: " + missingDocs);

        System.err.println("nDCG at " + k + " is: " + nDCG);
    }

    private ArrayList<PostingsList> calculate_tf_idf(Query query, NormalizationType normType) {
        long startTime = System.currentTimeMillis();

        ArrayList<PostingsList> terms = new ArrayList<PostingsList>();
        for (int i = 0, size = query.size(); i < size; i++) {
            String token = query.queryterm.get(i).term;
            Double weight = query.queryterm.get(i).weight;
            PostingsList p = index.getPostings(token);
            if (p == null) 
                continue;
            double df_t = p.size();
            double idf_t = Math.log((double)index.corpusSize()/df_t);

            for (int d = 0; d < p.size(); d++) {
                PostingsEntry entry = p.get(d);
                double tf_dt = entry.offset.size() * weight;
                
                double norm = -1;

                if (normType == NormalizationType.NUMBER_OF_WORDS)
                    norm = (double)Index.docLengths.get(entry.docID);
                else if (normType == NormalizationType.EUCLIDEAN)
                    norm = Index.euclidianLength.get(getFileName(Index.docNames.get(entry.docID)));

                double tf_idf_t = (tf_dt * idf_t) / norm;

                entry.score = tf_idf_t;
            }
            terms.add(p);
        }

        // long elapsedTime = System.currentTimeMillis() - startTime;
        // System.err.println("TF_IDFS took: " + elapsedTime + " ms");

        return terms;
    }

    private void calculate_page_ranking(ArrayList<PostingsList> list) {
        for (PostingsList p : list) {
            for (int d = 0; d < p.size(); d++) {
                PostingsEntry entry = p.get(d);
                entry.score = Index.pageRanking.get(getFileName(Index.docNames.get(entry.docID)));
            }
        }
    }

    private void calculate_combined_ranking(ArrayList<PostingsList> list, NormalizationType normType, double ratio) {
        // could probably use the above functions to make it nicer but im lazy and copy pasta
        HashMap<PostingsEntry, Double> tf_idfs = new HashMap<PostingsEntry, Double>();
        HashMap<PostingsEntry, Double> page_ranks = new HashMap<PostingsEntry, Double>();
        double tf_idfs_sum = 0;
        double page_ranks_sum = 0;
        for (PostingsList p : list) {
            double df_t = p.size();
            double idf_t = Math.log(index.corpusSize()/df_t);

            for (int d = 0; d < p.size(); d++) {
                PostingsEntry entry = p.get(d);
                double pageRank = Index.pageRanking.get(getFileName(Index.docNames.get(entry.docID)));
                page_ranks.put( entry, pageRank );
                page_ranks_sum += pageRank;

                double tf_dt = entry.offset.size();      
                double norm = -1;
                if (normType == NormalizationType.NUMBER_OF_WORDS)
                    norm = Index.docLengths.get(entry.docID);
                else if (normType == NormalizationType.EUCLIDEAN)
                    norm = Index.euclidianLength.get(getFileName(Index.docNames.get(entry.docID)));      
                double tf_idf_t = (tf_dt * idf_t) / norm;
                tf_idfs.put( entry, tf_idf_t );
                tf_idfs_sum += tf_idf_t;
            }
        }

        // Normalize both scores and set them
        for (PostingsList p : list) {
            for (int d = 0; d < p.size(); d++) {
                PostingsEntry entry = p.get(d);
                entry.score = ratio * tf_idfs.get(entry) / tf_idfs_sum + 
                              (1-ratio) * page_ranks.get(entry) / page_ranks_sum;
            }
        }
    }

    private void calculate_hits_ranking(ArrayList<PostingsList> list, double ratio) {
        Iterator<PostingsList> piter = list.iterator();
        PostingsList plist;
        if (piter.hasNext())
            plist = piter.next();
        else
            return;

        while (piter.hasNext()) {
            Iterator<PostingsEntry> eiter = piter.next().iterator();
            PostingsEntry e;
            while (eiter.hasNext()) {
                e = eiter.next();
                if (!plist.containsDocID(e.docID)) {
                    plist.insert(e.docID, e.score, 0);
                }
            }
        }

        plist = hitsRanker.rank(plist, ratio);
        list.clear();
        list.add(plist);
    }


    private PostingsList phrase_query(Query query) {
        ArrayList<PostingsList> tokens = new ArrayList<PostingsList>();
        for (int i = 0, size = query.size(); i < size; i++)
        {
            String token = query.queryterm.get(i).term;
            tokens.add(index.getPostings(token));
        }

        Iterator<PostingsList> iter = tokens.iterator();
        PostingsList res = iter.next();

        while (iter.hasNext() && res != null) {
            res = positional_intersection(res, iter.next());
        }

        return res;
    }

    private PostingsList intersection_query(Query query) {
        ArrayList<PostingsList> tokens = new ArrayList<PostingsList>();
        PostingsList p;
        for (int i = 0, size = query.size(); i < size; i++)
        {
            String token = query.queryterm.get(i).term;
            p = index.getPostings(token);
            if (p == null) {
                return null;
            }
            tokens.add(p);
        }
        Collections.sort(tokens, Comparator.comparingInt(PostingsList::size)); // sort the list in acending order

        PostingsList res = tokens.get(0);
        Iterator<PostingsList> iter = tokens.iterator();
        iter.next(); // skip 0

        while (iter.hasNext() && res != null) {
            res = intersection(res, iter.next());
        }

        return res;
    }

    private PostingsList positional_intersection(PostingsList p1, PostingsList p2) {
        PostingsList ret = new PostingsList();
        int p1i = 0, p2i = 0;
        PostingsEntry pe1, pe2;
        
        while (p1i < p1.size() && p2i < p2.size()) {
            pe1 = p1.get(p1i);
            pe2 = p2.get(p2i);
            if (pe1.docID == pe2.docID) {
                int po1i = 0, po2i = 0;
                int offset1, offset2;
                
                while (po1i < pe1.offset.size() && po2i < pe2.offset.size()) { // Search for words next to each other
                    offset1 = pe1.offset.get(po1i);
                    offset2 = pe2.offset.get(po2i);
                    if (offset1+1 == offset2) {
                        ret.insert(pe1.docID, 0, offset2);
                        po1i++; po2i++;
                    } else if (offset1 < offset2) {
                        po1i++;
                    } else {
                        po2i++;
                    }
                }
                
                p1i++; p2i++;
            } else if (pe1.docID < pe2.docID) {
                p1i++;
            } else {
                p2i++;
            }
        }

        if (ret.size() > 0){
            return ret;
        }
        
        return null;
    }
    
    private PostingsList intersection(PostingsList p1, PostingsList p2) {
        PostingsList ret = new PostingsList();
        int i = 0, j = 0;
        while (i < p1.size() && j < p2.size()) {
            if (p1.get(i).docID == p2.get(j).docID) {
                ret.insert(p1.get(i).docID, 0, 0); // offset doesnt matter right now 
                i++; j++;
            } else if (p1.get(i).docID < p2.get(j).docID) {
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
     *  Returns the filename at the end of a path.
     */
    private String getFileName(String path) {
        String result = "";
        StringTokenizer tok = new StringTokenizer( path, "\\/" );
        while ( tok.hasMoreTokens() ) {
            result = tok.nextToken();
        }
        return result;
    }
}