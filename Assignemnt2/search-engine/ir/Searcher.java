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
import java.util.Iterator;
import java.lang.Math;

/**
 *  Searches an index for results of a query.
 */
public class Searcher {

    /** The index to be searched by this Searcher. */
    Index index;

    /** The k-gram index to be searched by this Searcher */
    KGramIndex kgIndex;
    
    /** Constructor */
    public Searcher( Index index, KGramIndex kgIndex ) {
        this.index = index;
        this.kgIndex = kgIndex;
    }

    /**
     *  Searches the index for postings matching the query.
     *  @return A postings list representing the result of the query.
     */
    public PostingsList search( Query query, QueryType queryType, RankingType rankingType, NormalizationType normType ) { 
        //
        //  REPLACE THE STATEMENT BELOW WITH YOUR CODE
        // 
        PostingsList ret = null;

        switch (queryType) {
            case INTERSECTION_QUERY:
                ret = intersection_query(query);
                break;

            case PHRASE_QUERY:
                ret = phrase_query(query);
                break;

            case RANKED_QUERY:
                ret = ranked_query(query);
                break;
        
            default:
                System.err.println("Some error occured in Searcher.java (wrong queryType)");
                break;
        }
        
        return ret;
    }

    private PostingsList ranked_query(Query query) {
        ArrayList<PostingsList> terms = new ArrayList<PostingsList>();
        for (int i = 0, size = query.size(); i < size; i++)
        {
            String token = query.queryterm.get(i).term;
            PostingsList p = index.getPostings(token);
            terms.add(p);
        }
        
        for (PostingsList p : terms) {
            caluclate_scores(p);
        }

        // merge
        Iterator<PostingsList> piter = terms.iterator();
        PostingsList res = piter.next();

        while (piter.hasNext()) {
            Iterator<PostingsEntry> eiter = piter.next().iterator();
            PostingsEntry e;
            while (eiter.hasNext()) {
                e = eiter.next();
                if (res.containsDocID(e.docID)) {
                    res.addScore(e.docID, e.score);
                } else {
                    res.insert(e.docID, e.score, 0);
                }
            }
        }

        res.sortByScores();
        return res;
    }

    private void caluclate_scores(PostingsList p) {
        // Calculate w_t,q
        // For each pair(d, tf_t,d) in postings list
        // add wf_t,d to score of d

        double df_t = p.size();
        double idf_t = Math.log(index.corpusSize()/df_t);

        for (int d = 0; d < p.size(); d++) {
            PostingsEntry entry = p.get(d);
            double tf_dt = entry.offset.size();            
            
            double tf_idf_t = (tf_dt * idf_t) / Index.docLengths.get(entry.docID);

            entry.score = tf_idf_t;
        }
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
}