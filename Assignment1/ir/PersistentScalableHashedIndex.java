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
    /** The temp-merge dictionary file name */
    public static final String MERGE_DICTIONARY_FNAME = "dictionary_merge";

    /** The temp-merge data file name */
    public static final String MERGE_DATA_FNAME = "data_merge";

    public static final int BATCHSIZE = 100_000;
    
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
        } catch (EOFException e) {
            return null;
        } catch ( IOException e ) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     *  Inserts this token in the main-memory hashtable.
     */
    private int lastDocID = -1;
    public void insert( String token, int docID, int offset ) {
        if (index.size() >= BATCHSIZE && lastDocID != docID) {
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

        lastDocID = docID;
    }

    /**
     *  Write index to file after indexing is done.
     */
    @Override
    public void cleanup() {
        System.err.println( index.keySet().size() + " unique words" );
 
        String merge_data_location = INDEXDIR + "/" + MERGE_DATA_FNAME;
        String merge_dict_location = INDEXDIR + "/" + MERGE_DICTIONARY_FNAME;

        // Wait until file is gone
        File f2 = new File(merge_data_location);
        if (f2.length() > 0) {
            try {
                System.err.print("Waiting for merge to finish....");
                while (f2.length() > 0) {
                    // it will sleep the main thread for 1 sec
                    Thread.sleep(1000);
                    f2 = new File(merge_data_location);
                }
            }
            catch (Exception e) {
                System.err.println(e);
            }
            System.err.print("Merge done...");
        }

        
        File f = new File(INDEXDIR + "/" + DATA_FNAME); // If one index file already exists, merge
        boolean merge = f.length() > 0;
        
        System.err.print( "Writing index to disk... ");

        if (merge) {
            try {
                readDocInfo(); // Add the previous info into mem, save it
            } catch ( FileNotFoundException e ) {
            } catch ( IOException e ) {
                e.printStackTrace();
            }
            writeIndex();
            merge_files();
        } else {
            writeIndex();
        }

        // Make a new files and reset
        free = 0L;
        index.clear();
        docLengths.clear();
        docNames.clear();
        try {
            dictionaryFile = new RandomAccessFile(merge_dict_location, "rw" );
            dataFile = new RandomAccessFile(merge_data_location, "rw" );
        } catch ( IOException e ) {
            e.printStackTrace();
        }

        System.err.println( "done!" );
    }

    private void merge_files() {
        String main_data_location = INDEXDIR + "/" + DATA_FNAME;
        String main_dict_location = INDEXDIR + "/" + DICTIONARY_FNAME; 
        String merge_data_location = INDEXDIR + "/" + MERGE_DATA_FNAME;
        String merge_dict_location = INDEXDIR + "/" + MERGE_DICTIONARY_FNAME;

        RandomAccessFile tempData = null;
        RandomAccessFile tempDict = null;
        RandomAccessFile main_data = null;
        RandomAccessFile main_dict = null;
        RandomAccessFile merge_data = null;
        RandomAccessFile merge_dict = null;

        try {
            tempData = new RandomAccessFile( INDEXDIR + "/tempdata", "rw" );
            tempDict = new RandomAccessFile( INDEXDIR + "/tempdict", "rw" );
            main_data = new RandomAccessFile(main_data_location, "rw" );
            main_dict = new RandomAccessFile(main_dict_location, "rw" );
            merge_data = new RandomAccessFile(merge_data_location, "rw" );
            merge_dict = new RandomAccessFile(merge_dict_location, "rw" );
        } catch ( IOException e ) {
            e.printStackTrace();
        }

        long free_ptr = 0;

        String[] main_tokens = get_tokens(main_dict, main_data);
        String[] merge_tokens = get_tokens(merge_dict, merge_data);

        // Store all of the data from the main data file (Usually bigger)
        int[] hashes_used = new int[(int)TABLESIZE];
        for (String token : main_tokens) {
            String[] main_sdata = get_positings_data(main_dict, main_data, token);
            String[] merge_sdata = null;
            String write_data = String.join(",", main_sdata);
            if (contains_token(merge_tokens, token)) {
                merge_sdata = get_positings_data(merge_dict, merge_data, token);
                merge_sdata[0] = "";
                write_data += String.join(",", merge_sdata);
            }
            

            // Write the data to the datafile
            int written_data = writeData(tempData, write_data, free_ptr);
            // Save the starting pointer and ending pointer
            Entry e = new Entry(free_ptr, free_ptr+written_data); // this is the postings list for token "key" 
            // Increment the starting pointer
            free_ptr += written_data+1; // +1 ?
            // Get hash of token
            int hash = hash_function(token);
            // Get the pointer corresponding to the location in the dictionary
            long ptr = get_pointer_from_hash(hash);
            
            if (hashes_used[hash] == 0) {
                writeEntry(tempDict, e, ptr);
                hashes_used[hash] = 1;
            } else {
                // Get a new pointer to store the current postings list
                int new_hash = find_first_collision_free(hashes_used);
                long new_ptr = get_pointer_from_hash(new_hash);
                
                // Get the entry at the collision hash value
                EndOfListResponse response = find_end_of_list(tempDict, ptr);
                // Save the new pointer to the collision entry
                Entry col_entry = response.entry;
                col_entry.collision_ptr = new_ptr;
                writeEntry(tempDict, col_entry, response.ptr);
                
                // Write the entry to its new position
                writeEntry(tempDict, e, new_ptr);
                hashes_used[new_hash] = 1;
                // The above basically creates a linked list
            }
        }

        for (String token : merge_tokens) {
            if (contains_token(main_tokens, token)){
                continue;
            }
            
            String merge_sdata[] = get_positings_data(merge_dict, merge_data, token);
            String write_data = String.join(",", merge_sdata);
            

            // Write the data to the datafile
            int written_data = writeData(tempData, write_data, free_ptr);
            // Save the starting pointer and ending pointer
            Entry e = new Entry(free_ptr, free_ptr+written_data); // this is the postings list for token "key" 
            // Increment the starting pointer
            free_ptr += written_data+1; // +1 ?
            // Get hash of token
            int hash = hash_function(token);
            // Get the pointer corresponding to the location in the dictionary
            long ptr = get_pointer_from_hash(hash);
            
            if (hashes_used[hash] == 0) {
                writeEntry(tempDict, e, ptr);
                hashes_used[hash] = 1;
            } else {
                // Get a new pointer to store the current postings list
                int new_hash = find_first_collision_free(hashes_used);
                long new_ptr = get_pointer_from_hash(new_hash);
                
                // Get the entry at the collision hash value
                EndOfListResponse response = find_end_of_list(tempDict, ptr);
                // Save the new pointer to the collision entry
                Entry col_entry = response.entry;
                col_entry.collision_ptr = new_ptr;
                writeEntry(tempDict, col_entry, response.ptr);
                
                // Write the entry to its new position
                writeEntry(tempDict, e, new_ptr);
                hashes_used[new_hash] = 1;
                // The above basically creates a linked list
            }
        }

        // Delete files and rename temp
        try {
            main_data.close();
            main_dict.close();
            merge_data.close();
            merge_dict.close();

            File f1 = new File(main_data_location);
            File f2 = new File(main_dict_location);
            File f3 = new File(merge_data_location);
            File f4 = new File(merge_dict_location);

            f1.delete();
            f2.delete();
            f3.delete();
            f4.delete();

            tempData.close();
            tempData.close();
            
            File f5 = new File(INDEXDIR + "/tempdata");
            File f6 = new File(INDEXDIR + "/tempdict");

            f5.renameTo(new File(main_data_location));
            f6.renameTo(new File(main_dict_location));
        } catch ( IOException e ) {
            e.printStackTrace();
        }

        /*
         * Read all the tokens
         * Take main tokens, see if the new file has that token
         * Combine the results and save
         * Finally save the new tokens (no need to combine, just check collisions)
         * Destroy all files and rename temp to main dict file.
         */
    }

    protected EndOfListResponse find_end_of_list(RandomAccessFile file, long ptr) {
        Entry e = new Entry(0, 0);
        while (true) {
            // Get the entry at the collision hash value
            e = readEntry(file, ptr);
            if (e.collision_ptr == -1) {
                break; // found the end of the linked list
            } else {
                ptr = e.collision_ptr;
            }
        }

        return new EndOfListResponse(e, ptr);
    }

    private String[] get_positings_data(RandomAccessFile dict, RandomAccessFile data, String token) {
        int hash = hash_function(token);
        long ptr = get_pointer_from_hash(hash);
        String ret_data[];

        while (true) {
            Entry e = readEntry(dict, ptr);
            ret_data = readData(data, e.start_ptr, (int)(e.end_ptr-e.start_ptr)).split(",");

            ptr = e.collision_ptr;
            
            if (ret_data[0].equals(token))
                break;

            if (e.collision_ptr == -1) {
                System.err.println("Null!!!");
                System.err.println(token);
                System.err.println(e.start_ptr);
                System.err.println(e.end_ptr);
            }

            if (ptr == -1)                
                return null; // token does not exsist
        }

        return ret_data;
    }

    private boolean contains_token(String[] arr, String token) {
        int index = Arrays.binarySearch(arr, token);

        return index>0;
    }

    private String[] get_tokens(RandomAccessFile dict, RandomAccessFile data) {
        ArrayList<String> ret = new ArrayList<String>();

        int hash = 0;
        while (hash < TABLESIZE) {
            long ptr = get_pointer_from_hash(hash);
            Entry e = readEntry(dict, ptr);

            if (e == null) // end of dict file
            {
                break;
            }
            
            if (e.end_ptr > 0) {
                String sdata[] = readData(data, e.start_ptr, (int)(e.end_ptr-e.start_ptr)).split(",");
                ret.add(sdata[0]);
            }

            hash++;
        }

        String[] sret = ret.toArray(new String[ret.size()]);
        Arrays.parallelSort(sret);
        return sret;
    }
}
