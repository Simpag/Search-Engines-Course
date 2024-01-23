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
                
                break;
        
            default:
                System.err.println("Some error occured in Searcher.java (wrong queryType)");
                break;
        }
        
        return ret;
    }

    private PostingsList phrase_query(Query query) {
        ArrayList<PostingsList> tokens = new ArrayList<PostingsList>();
        for (int i = 0, size = query.size(); i < size; i++)
        {
            String token = query.queryterm.get(i).term;
            tokens.add(index.getPostings(token));
        }
        //Collections.sort(tokens, Comparator.comparingInt(PostingsList::size)); // sort the list in acending order

        PostingsList res = tokens.get(0);
        Iterator<PostingsList> iter = tokens.iterator();
        iter.next(); // skip 0

        while (iter.hasNext() && res != null) {
            res = positional_intersection(res, iter.next());
        }

        return res;
    }

    private PostingsList intersection_query(Query query) {
        ArrayList<PostingsList> tokens = new ArrayList<PostingsList>();
        for (int i = 0, size = query.size(); i < size; i++)
        {
            String token = query.queryterm.get(i).term;
            tokens.add(index.getPostings(token));
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
        
        Iterator<PostingsEntry> p1_iter = p1.iterator();
        Iterator<PostingsEntry> p2_iter = p2.iterator();
        
        PostingsEntry _p1 = p1_iter.next();
        PostingsEntry _p2 = p2_iter.next();
        int iiii = 0;
        while (p1_iter.hasNext() && p2_iter.hasNext()) { // misses one document...?

            if (_p1.docID == _p2.docID) {
                Iterator<Integer> _pp1 = _p1.offset.iterator();
                Iterator<Integer> _pp2 = _p2.offset.iterator();
                int offset1 = _pp1.next();
                int offset2 = _pp2.next();
                
                while (_pp1.hasNext() && _pp2.hasNext()) {
                    if (offset1+1 == offset2) {
                        System.err.println(_p1.docID);
                        ret.insert(_p1.docID, offset2);
                        offset1 = _pp1.next();
                        offset2 = _pp2.next();
                    } else if (offset1 < offset2) {
                        offset1 = _pp1.next();
                    } else {
                        offset2 = _pp2.next();
                    }
                }

                System.err.println(iiii++);

                _p1 = p1_iter.next();
                _p2 = p2_iter.next();
            } else if (_p1.docID < _p2.docID) {
                _p1 = p1_iter.next();
            } else {
                _p2 = p2_iter.next();
            }
        }

        if (ret.size() > 0) {
            return ret;
        }
        
        return null;
    }
    
    private PostingsList intersection(PostingsList p1, PostingsList p2) {
        PostingsList ret = new PostingsList();
        int i = 0, j = 0;
        while (i < p1.size() && j < p2.size()) {
            if (p1.get(i).docID == p2.get(j).docID) {
                ret.insert(p1.get(i).docID, 0); // offset doesnt matter right now 
                i++; j++;
            } else if (p1.get(i).docID < p2.get(j).docID) {
                i++;
            } else {
                j++;
            }
        }

        if (ret.size() > 0){
            return ret;
        }
        
        return null;
    }
}