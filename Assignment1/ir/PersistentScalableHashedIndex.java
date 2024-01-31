/*  
 *   This file is part of the computer assignment for the
 *   Information Retrieval course at KTH.
 * 
 *   Johan Boye, KTH, 2018
 */  

package ir;

import java.io.*;
import java.util.*;
import java.nio.ByteBuffer;
import java.nio.charset.*;
import java.util.concurrent.ThreadLocalRandom;



/*
 *   Implements an inverted index as a hashtable on disk.
 *   
 *   Both the words (the dictionary) and the data (the postings list) are
 *   stored in RandomAccessFiles that permit fast (almost constant-time)
 *   disk seeks. 
 *
 *   When words are read and indexed, they are first put in an ordinary,
 *   main-memory HashMap. When all words are read, the index is committed
 *   to disk.
 */
public class PersistentScalableHashedIndex extends PersistentHashedIndex {
    /** The directory where the persistent index files are stored. */
    public static final String INDEXDIR = "./scalable_index";

    /** The dictionary hash table on disk can fit this many entries. */
    public static final long TABLESIZE = 611953L;

    public static final int BATCHSIZE = 10_000_000;
    
    private String[] current_index_file = {"",""};

    /**
     *  Inserts this token in the main-memory hashtable.
     */
    public void insert( String token, int docID, int offset ) {
        if (index.size() >= BATCHSIZE) {
            cleanup();
        }

        if (index.containsKey(token)) {
            PostingsList p = index.get(token);
            p.insert(docID, 0, offset);
        } else {
            PostingsList p = new PostingsList();
            p.insert(docID, 0, offset);
            index.put(token, p);
        }
    }


    /**
     *  Write index to file after indexing is done.
     */
    public void cleanup() {
        System.err.println( index.keySet().size() + " unique words" );
        System.err.print( "Writing index to disk..." );
        
        File f = new File(INDEXDIR + "/" + DATA_FNAME); // If one index file exists
        boolean merge = f.isFile();
        
        writeIndex();

        if (merge) {
            merge_files();
        }

        // Make a new files and reset
        String i = Integer.toString(ThreadLocalRandom.current().nextInt(0, 100 + 1));
        String data_file = INDEXDIR + "/" + DATA_FNAME + i;
        String dict_file = INDEXDIR + "/" + DICTIONARY_FNAME + i;
        current_index_file[0] = data_file;
        current_index_file[1] = dict_file;
        
        free = 0L;
        index.clear();
        docLengths.clear();
        docNames.clear();
        try {
            dictionaryFile = new RandomAccessFile(dict_file, "rw" );
            dataFile = new RandomAccessFile(data_file, "rw" );
        } catch ( IOException e ) {
            e.printStackTrace();
        }

        System.err.println( "done!" );
    }

    private void merge_files() {
        String main_data_file = INDEXDIR + "/" + DATA_FNAME;
        String main_dict_file = INDEXDIR + "/" + DICTIONARY_FNAME; 
        String merge_data_file = current_index_file[0];
        String merge_dict_file = current_index_file[1];

        // think about the current index file

        try {
            RandomAccessFile tempData = new RandomAccessFile(merge_data_file, "rw" );
            RandomAccessFile tempDict = new RandomAccessFile(merge_dict_file, "rw" );
        } catch ( IOException e ) {
            e.printStackTrace();
        }

        long main_pointer = 0L, merge_pointer = 0L;

        while (true) {
            Entry main_entry = find_first_entry_from(main_dict_file, main_pointer);
            Entry merge_entry = find_first_entry_from(merge_dict_file, merge_pointer);
        }

        // Do some merging
        /*
         * Find a non-empty token in data_file
         * Hash that token and check if it exists in the main file
         * If it exists, merge the two list by appending the docId's and offsets (should just need to add to the end since docID is increasing)
         * Then replace the end pointer in the dict entry
         * If it does not exists, just save it to the empty bucket and append to the datafile
         * 
         * Wont work because I would have to change all end pointer forward
         */

         /*
         * Create a new temporary data and dict file
         * Read first non-empty token from both files
         * If they are the same - combine them - else just save the lower valued token
         * Increase the pointer to the lower valued token until a new token is found and repeat
         * 
         * Once done, remove old files and remane the temporary file to the final name.
         */
    }

    private Entry find_first_entry_from(String file, long ptr) {
        return null;
    }
}
