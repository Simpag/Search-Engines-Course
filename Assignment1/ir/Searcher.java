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
                
                break;

            case RANKED_QUERY:
                
                break;
        
            default:
                System.err.println("Some error occured in Searcher.java (wrong queryType)");
                break;
        }
        
        return ret;
    }

    private PostingsList intersection_query(Query query) {
        ArrayList<PostingsList> list = new ArrayList<PostingsList>();
        for (int i = 0, size = query.size(); i < size; i++)
        {
            String token = query.queryterm.get(i).term;
            list.add(index.getPostings(token));
        }
        Collections.sort(list, Comparator.comparingInt(PostingsList::size)); // sort the list in acending order

        PostingsList res = list.get(0);
        Iterator<PostingsList> iter = list.iterator();
        iter.next(); // skip 0

        while (iter.hasNext() && res != null) {
            res = intersection(res, iter.next());
        }

        return res;
    }

    private PostingsList intersection(PostingsList p1, PostingsList p2) {
        PostingsList ret = new PostingsList();
        int i = 0, j = 0;
        while (i < p1.size() && j < p2.size()) {
            if (p1.get(i).docID == p2.get(j).docID) {
                ret.insert(p1.get(i).docID, 0); // offset doesnt matter right now 
                i++;
                j++;
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