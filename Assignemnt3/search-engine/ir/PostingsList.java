/*  
 *   This file is part of the computer assignment for the
 *   Information Retrieval course at KTH.
 * 
 *   Johan Boye, 2017
 */  

package ir;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.StringJoiner;

public class PostingsList {
    
    /** The postings list */
    private ArrayList<PostingsEntry> list = new ArrayList<PostingsEntry>();

    private static final String DATA_SEPARATOR = " ";

    /** Number of postings in this list. */
    public int size() {
        return list.size();
    }

    /** Returns the ith posting. */
    public PostingsEntry get( int i ) {
        return list.get( i );
    }

    /** Returns the posting with docID. */
    public PostingsEntry getDoc( int docID ) {
        for (PostingsEntry e : list) {
            if (e.docID == docID)
                return e;
        }
        return null;
    }

    // 
    //  YOUR CODE HERE - done?
    //
    public Iterator<PostingsEntry> iterator() {
        return list.iterator();
    }

    private PostingsEntry last_entry = new PostingsEntry(-1, 0, 0);
    public void insert(int docID, double score, int offset) {
        PostingsEntry p;
        if (docID == last_entry.docID) { // if the docID for this token already exists
            p = last_entry;
            p.offset.add(offset); // just add the new offset (where the token appears)
        } else {
            p = new PostingsEntry(docID, score, offset);
            list.add(p);
        }

        last_entry = p;
    }

    public String serialize(String token)
    {
        // Stored as first entry is token, then docID,score,#offsets,offsets and repeating
        String ret = token+DATA_SEPARATOR;
        for (int i = 0; i < list.size(); i++) {
            ret += list.get(i).serialize();
            if (i < list.size()-1) // add a DATA_SEPARATOR to all but last
                ret += DATA_SEPARATOR;
        }

        return ret;
    }

    public static PostingsList deserialize(String[] s) {
        PostingsList p = new PostingsList();

        int i = 1;
        while (i < s.length) {
            int docID = Integer.valueOf(s[i++]);
            double score = Double.valueOf(s[i++]);
            int number_of_offsets = Integer.valueOf(s[i++]);
            for (int j = 0; j < number_of_offsets; j++) {
                int offset = Integer.valueOf(s[i++]);
                p.insert(docID, score, offset);
            }
        }

        //Collections.sort(p.list, (o1, o2) -> o1.docID - o2.docID);

        return p;
    }

    public void sortByScores() {
        list.sort((o1, o2) -> Double.compare(o2.score, o1.score));
    }

    public void resetScores() {
        for (PostingsEntry e : list) {
            e.score = 0;
        }
    }

    public void addScore(int docID, double score) {
        for (PostingsEntry e : list) {
            if (e.docID == docID)
                e.score += score;
        }
    }

    public boolean containsDocID(int docID) {
        for (PostingsEntry e : list) {
            if (e.docID == docID)
                return true;
        }
        return false;
    }
}

