/*  
 *   This file is part of the computer assignment for the
 *   Information Retrieval course at KTH.
 * 
 *   Johan Boye, 2017
 */  

package ir;

import java.util.ArrayList;

public class PostingsList {
    
    /** The postings list */
    private ArrayList<PostingsEntry> list = new ArrayList<PostingsEntry>();


    /** Number of postings in this list. */
    public int size() {
    return list.size();
    }

    /** Returns the ith posting. */
    public PostingsEntry get( int i ) {
    return list.get( i );
    }

    // 
    //  YOUR CODE HERE - done?
    //

    private int last_docID = -1;
    public void insert(int docID, int offset) {
        double score = 0;
        if (docID == last_docID) { // if the docID for this token already exists
            find_entry(docID).offset.add(offset);
        } else {
            PostingsEntry p = new PostingsEntry(docID, score, offset);
            list.add(p);
        }

        last_docID = docID;
    }

    private PostingsEntry find_entry(int docID) {
        for (int i = 0, size = list.size(); i < size; i++)
        {
            PostingsEntry p = list.get(i);
            if (p.docID == docID) {
                return p;
            }
        }
        return null;
    }

    /*
    public void insert(int docID, int offset) {
        double score = 0;
        PostingsEntry _p = contains_docID(docID);
        if (_p != null) { // if the docID for this token already exists
            _p.offset.add(offset);
        } else {
            PostingsEntry p = new PostingsEntry(docID, score, offset);
            list.add(p);
        }
    }

    
    private PostingsEntry contains_docID(int docID) {
        for (PostingsEntry p : list) { 		      
            if (p.docID == docID) {
                return p;
            } 		
        }
        return null;
    }
     */
}

