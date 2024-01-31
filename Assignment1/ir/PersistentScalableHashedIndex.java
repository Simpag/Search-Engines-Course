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
     *  Writes data to the data file at a specified place.
     *
     *  @return The number of bytes written.
     */ 
    int writeData(RandomAccessFile file, String dataString, long ptr ) {
        try {
            file.seek( ptr ); 
            byte[] data = dataString.getBytes();
            file.write( data );
            return data.length;
        } catch ( IOException e ) {
            e.printStackTrace();
            return -1;
        }
    }


    /**
     *  Reads data from the data file
     */ 
    String readData(RandomAccessFile file, long ptr, int size ) {
        try {
            file.seek( ptr );
            byte[] data = new byte[size];
            file.readFully( data );
            return new String(data);
        } catch ( IOException e ) {
            e.printStackTrace();
            return null;
        }
    }

    /*
     *  Writes an entry to the dictionary hash table file. 
     *
     *  @param entry The key of this entry is assumed to have a fixed length
     *  @param ptr   The place in the dictionary file to store the entry
     */
    void writeEntry(RandomAccessFile file, Entry entry, long ptr) {
        try {
            file.seek(ptr); 
            byte[] data = entry.get_bytes();
            file.write(data);
            return;
        } catch ( IOException e ) {
            e.printStackTrace();
            return;
        }
    }

    /**
     *  Reads an entry from the dictionary file.
     *
     *  @param ptr The place in the dictionary file where to start reading.
     */
    Entry readEntry(RandomAccessFile file, long ptr ) {   
        try {
            file.seek( ptr );
            byte[] data = new byte[Entry.byte_size];
            file.readFully( data );
            return new Entry(data);
        } catch ( IOException e ) {
            e.printStackTrace();
            return null;
        }
    }

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
            merge_files(null, null);
        }

        // Make a new files and reset
        String i = Integer.toString(ThreadLocalRandom.current().nextInt(0, 100 + 1));
        String data_file = INDEXDIR + "/" + DATA_FNAME + i;
        String dict_file = INDEXDIR + "/" + DICTIONARY_FNAME + i;
        //current_index_file[0] = data_file;
        //current_index_file[1] = dict_file;
        
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

    private void merge_files(RandomAccessFile data, RandomAccessFile dict) {
        String main_data_location = INDEXDIR + "/" + DATA_FNAME;
        String main_dict_location = INDEXDIR + "/" + DICTIONARY_FNAME; 
        //String merge_data_file = current_index_file[0];
        //String merge_dict_file = current_index_file[1];

        // think about the current index file
        RandomAccessFile tempData = null;
        RandomAccessFile tempDict = null;
        RandomAccessFile main_data = null;
        RandomAccessFile main_dict = null;

        try {
            tempData = new RandomAccessFile( INDEXDIR + "/tempdata", "rw" );
            tempDict = new RandomAccessFile( INDEXDIR + "/tempdict", "rw" );
            main_data = new RandomAccessFile(main_data_location, "rw" );
            main_dict = new RandomAccessFile(main_dict_location, "rw" );
        } catch ( IOException e ) {
            e.printStackTrace();
        }

        long main_pointer = 0L, merge_pointer = 0L;

        while (true) {
            EndOfListResponse main_resp = find_first_entry_from(main_dict, main_pointer);
            String main_entry_data[] = readData(main_data, main_resp.entry.start_ptr, (int)(main_resp.entry.end_ptr-main_resp.entry.start_ptr)).split(",");

            
            

            main_pointer = get_pointer_from_hash((int)(main_pointer+1));
            merge_pointer = get_pointer_from_hash((int)(merge_pointer+1));
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

    private EndOfListResponse find_first_entry_from(RandomAccessFile dict, long ptr) {
        while (ptr < TABLESIZE) {
            Entry e = readEntry(dict, ptr);

            if (e.end_ptr == 0L) {
                ptr = get_pointer_from_hash((int)(ptr+1));
            } else {
                return new EndOfListResponse(e, ptr);
            }
        }

        return null;
    }
}
