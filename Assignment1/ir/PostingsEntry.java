/*  
 *   This file is part of the computer assignment for the
 *   Information Retrieval course at KTH.
 * 
 *   Johan Boye, 2017
 */  

package ir;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.io.Serializable;

public class PostingsEntry implements Comparable<PostingsEntry>, Serializable {

    public int docID;
    public double score = 0;

    private static final String DATA_SEPARATOR = " ";

    /**
     *  PostingsEntries are compared by their score (only relevant
     *  in ranked retrieval).
     *
     *  The comparison is defined so that entries will be put in 
     *  descending order.
     */
    public int compareTo( PostingsEntry other ) {
       return Double.compare( other.score, score );
    }


    //
    // YOUR CODE HERE - done?
    //
    public ArrayList<Integer> offset = new ArrayList<Integer>();
    public PostingsEntry(int docID, double score, int offset) {
        this.docID = docID;
        this.score = score;
        this.offset.add(offset);
    }

    public String serialize()
    {
        // Returns docID,score,#offsets,offsets
        String ret = String.valueOf(docID)+DATA_SEPARATOR;
        ret += String.valueOf(score)+DATA_SEPARATOR;
        ret += String.valueOf(offset.size());
        for (int o : offset) {
            ret += DATA_SEPARATOR+String.valueOf(o);
        }

        return ret;
    }

    /*@Override
    public boolean equals (Object object) {
        if (object != null && object.getClass() == this.getClass()) {
            PostingsEntry p = (PostingsEntry) object;
            if (this.docID == p.docID) {
                return true;
            }
        }
        return false;
    } making a loop instead of using .contains()*/
}

