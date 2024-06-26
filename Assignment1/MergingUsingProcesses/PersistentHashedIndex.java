/*  
 *   This file is part of the computer assignment for the
 *   Information Retrieval course at KTH.
 * 
 *   Johan Boye, KTH, 2018
 */  

package ir;

import java.io.*;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.*;
import java.security.spec.ECFieldF2m;
import java.time.LocalDateTime; 


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
public class PersistentHashedIndex implements Index {

    /** The directory where the persistent index files are stored. */
    public static final String INDEXDIR = "./guardian_index"; // "./index";

    /** The dictionary file name */
    public static final String DICTIONARY_FNAME = "dictionary";

    /** The data file name */
    public static final String DATA_FNAME = "data";

    /** The terms file name */
    public static final String TERMS_FNAME = "terms";

    /** The doc info file name */
    public static final String DOCINFO_FNAME = "docInfo";

    /** The dictionary hash table on disk can fit this many entries. */
    public static final long TABLESIZE = 611_953L; //3_503_119L //611_953L

    /** The dictionary hash table is stored in this file. */
    RandomAccessFile dictionaryFile;
    RandomAccessFile readDictionaryFile;

    /** The data (the PostingsLists) are stored in this file. */
    RandomAccessFile dataFile;

    /** Pointer to the first free memory cell in the data file. */
    long free = 0L;

    /** The cache as a main-memory hash map. */
    HashMap<String,PostingsList> index = new HashMap<String,PostingsList>();

    public static final String DATA_SEPARATOR = " ";

    public LocalDateTime startTime;


    // ===================================================================

    /**
     *   A helper class representing one entry in the dictionary hashtable.
     */ 
    public static class Entry {
        //
        //  YOUR CODE HERE
        //
        public static final int byte_size = 3*Long.BYTES;
        public long start_ptr;
        public long end_ptr;
        public long collision_ptr;

        public Entry(long start, long end) {
            this.start_ptr = start;
            this.end_ptr = end;
            this.collision_ptr = -1L;
        }

        public Entry(byte[] data) {
            ByteBuffer buffer = ByteBuffer.wrap(data);
            this.start_ptr = buffer.getLong();
            this.end_ptr = buffer.getLong();
            this.collision_ptr = buffer.getLong();
        }

        public byte[] get_bytes() {
            ByteBuffer buffer = ByteBuffer.allocate(byte_size);
            buffer.putLong(start_ptr);
            buffer.putLong(end_ptr);
            buffer.putLong(collision_ptr);
            return buffer.array();
        }
    }


    // ==================================================================

    
    /**
     *  Constructor. Opens the dictionary file and the data file.
     *  If these files don't exist, they will be created. 
     */
    public PersistentHashedIndex() {
        try {
            dictionaryFile = new RandomAccessFile( INDEXDIR + "/" + DICTIONARY_FNAME, "rw" );
            readDictionaryFile = new RandomAccessFile( INDEXDIR + "/" + DICTIONARY_FNAME, "r");
            dataFile = new RandomAccessFile( INDEXDIR + "/" + DATA_FNAME, "rw" );
        } catch ( IOException e ) {
            e.printStackTrace();
        }

        try {
            readDocInfo();
        } catch ( FileNotFoundException e ) {
        } catch ( IOException e ) {
            e.printStackTrace();
        }
        
        startTime = LocalDateTime.now();
    }

    /**
     *  Writes data to the data file at a specified place.
     *
     *  @return The number of bytes written.
     */ 
    int writeData( String dataString, long ptr ) {
        try {
            dataFile.seek(ptr); 
            byte[] data = dataString.getBytes();
            dataFile.write( data );
            return data.length;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return -1;
    }


    /**
     *  Reads data from the data file
     */ 
    String readData( long ptr, int size ) {
        try {
            dataFile.seek( ptr );
            byte[] data = new byte[size];
            dataFile.readFully( data );
            return new String(data);
        } catch ( IOException e ) {
            e.printStackTrace();
            return null;
        }
    }


    // ==================================================================
    //
    //  Reading and writing to the dictionary file.

    /*
     *  Writes an entry to the dictionary hash table file. 
     *
     *  @param entry The key of this entry is assumed to have a fixed length
     *  @param ptr   The place in the dictionary file to store the entry
     */
    public static class writeBuffer {
        public byte[] data;
        public long ptr;
        public writeBuffer(byte[] d, long p) {
            data = d;
            ptr = p;
        }
    }
    ArrayList<writeBuffer> writeDictBuffer = new ArrayList<writeBuffer>();
    void writeEntry(Entry entry, long ptr) {
        //
        //  YOUR CODE HERE
        //
        byte[] data = entry.get_bytes();
        writeDictBuffer.add(new writeBuffer(data, ptr));
    }

    private void emptyDictBuffer() {
        try {
            Collections.sort(writeDictBuffer, (a,b)-> (int)(a.ptr-b.ptr));
            for (writeBuffer b : writeDictBuffer) {
                dictionaryFile.seek( b.ptr ); 
                dictionaryFile.write( b.data );
            }
            writeDictBuffer.clear();
        } catch ( IOException e ) {
            e.printStackTrace();
        }
    }

    /**
     *  Reads an entry from the dictionary file.
     *
     *  @param ptr The place in the dictionary file where to start reading.
     */
    Entry readEntry( long ptr ) {   
        //
        //  REPLACE THE STATEMENT BELOW WITH YOUR CODE 
        //
        try {
            readDictionaryFile.seek( ptr );
            byte[] data = new byte[Entry.byte_size];
            readDictionaryFile.readFully( data );
            return new Entry(data);
        } catch ( IOException e ) {
            e.printStackTrace();
            return null;
        }
    }


    // ==================================================================

    /**
     *  Writes the document names and document lengths to file.
     *
     * @throws IOException  { exception_description }
     */
    protected void writeDocInfo() throws IOException {
        FileOutputStream fout = new FileOutputStream( INDEXDIR + "/docInfo");
        for ( Map.Entry<Integer,String> entry : docNames.entrySet() ) {
            Integer key = entry.getKey();
            String docInfoEntry = key + ";" + entry.getValue() + ";" + docLengths.get(key) + "\n";
            fout.write( docInfoEntry.getBytes() );
        }
        fout.close();
    }

    protected void writeDocInfo(String fname) throws IOException {
        FileOutputStream fout = new FileOutputStream(fname);
        for ( Map.Entry<Integer,String> entry : docNames.entrySet() ) {
            Integer key = entry.getKey();
            String docInfoEntry = key + ";" + entry.getValue() + ";" + docLengths.get(key) + "\n";
            fout.write( docInfoEntry.getBytes() );
        }
        fout.close();
    }


    /**
     *  Reads the document names and document lengths from file, and
     *  put them in the appropriate data structures.
     *
     * @throws     IOException  { exception_description }
     */
    protected void readDocInfo() throws IOException {
        File file = new File( INDEXDIR + "/docInfo" );
        FileReader freader = new FileReader(file);
        try ( BufferedReader br = new BufferedReader(freader) ) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] data = line.split(";");
                docNames.put( new Integer(data[0]), data[1] );
                docLengths.put( new Integer(data[0]), new Integer(data[2]) );
            }
        }
        freader.close();
    }

    // ========================================================================
    /**
     *  Writes the tokens to file.
     *
     * @throws IOException  { exception_description }
     */
    protected void writeTerms(String[] terms, String fileName) {
        try {
            FileOutputStream fout = new FileOutputStream(fileName);
            //String[] terms = new String[index.size()];
            //terms = index.keySet().toArray(terms);
            Arrays.sort(terms);
            for (String token : terms) {
                String w = token + "\n";
                fout.write(w.getBytes());
            }
            fout.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected ArrayList<String> readTerms(String fileName) {
        ArrayList<String> ret = new ArrayList<String>();
        try {
            File file = new File(fileName);
            FileReader freader = new FileReader(file);
            BufferedReader br = new BufferedReader(freader);
            String line;
            while ((line = br.readLine()) != null) {
                ret.add(line);
            }
            freader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return ret;
    }

    /**
     *  Write the index to files.
     */
    public void writeIndex(boolean write_doc_info) {
        int collisions = 0;
        int[] hashes_used = new int[(int)TABLESIZE];
        try {
            // Write the 'docNames' and 'docLengths' hash maps to a file
            if (write_doc_info)
                writeDocInfo();

            // Write the dictionary and the postings list

            // 
            //  YOUR CODE HERE
            //
            for (String key : index.keySet()){
                // Write the data to the datafile
                int written_data = writeData(index.get(key).serialize(key), free);
                // Save the starting pointer and ending pointer
                Entry e = new Entry(free, free+written_data); // this is the postings list for token "key" 
                // Increment the starting pointer
                free += written_data; // +1 ?
                // Get hash of token
                int hash = hash_function(key);
                // Get the pointer corresponding to the location in the dictionary
                long ptr = get_pointer_from_hash(hash);
                
                if (hashes_used[hash] == 0) {
                    writeEntry(e, ptr);
                    hashes_used[hash] = 1;
                } else {
                    emptyDictBuffer();
                    // Collision occured
                    collisions += 1;
                    // Get a new pointer to store the current postings list
                    int new_hash = find_first_collision_free(hashes_used, hash);
                    long new_ptr = get_pointer_from_hash(new_hash);
                    
                    // Get the entry at the collision hash value
                    EndOfListResponse response = find_end_of_list(ptr);
                    // Save the new pointer to the collision entry
                    Entry col_entry = response.entry;
                    col_entry.collision_ptr = new_ptr;
                    writeEntry(col_entry, response.ptr);
                    
                    // Write the entry to its new position
                    writeEntry(e, new_ptr);
                    hashes_used[new_hash] = 1;
                    // The above basically creates a linked list
                }
            }
            emptyDictBuffer();
        } catch ( IOException e ) {
            e.printStackTrace();
        }
        System.err.println( collisions + " collisions." );
    }

    public static class EndOfListResponse {
        public Entry entry;
        public long ptr;

        public EndOfListResponse(Entry entry, long ptr) {
            this.entry = entry;
            this.ptr = ptr;
        }
    }
    protected EndOfListResponse find_end_of_list(long ptr) {
        Entry e = new Entry(0, 0);
        while (true) {
            // Get the entry at the collision hash value
            e = readEntry(ptr);
            if (e.collision_ptr == -1L) {
                break; // found the end of the linked list
            } else {
                ptr = e.collision_ptr;
            }
        }

        return new EndOfListResponse(e, ptr);
    }

    public static int find_first_collision_free(int[] arr, int hash) {
        /*int cnt = 0;

        while (cnt < 20) {
            int randomNum = ThreadLocalRandom.current().nextInt(0, (int)TABLESIZE);
            if (arr[randomNum] == 0) {
                return randomNum;
            }
            cnt++; 
        }*/
        
        /*for (int i = 0; i < arr.length; i++) {
            if (arr[i] == 0){
                return i;
            }
        }*/
        int cnt = -1;
        while (cnt < TABLESIZE) {
            if (arr[hash] == 0) {
                return hash;
            } 
            hash = (int)((hash+1)%TABLESIZE);
            cnt++;
        }
        
        System.err.println("Something went wrong, did not find any collision free indicies!");

        return -1;
    }


    // ==================================================================


    /**
     *  Returns the postings for a specific term, or null
     *  if the term is not in the index.
     */
    public PostingsList getPostings( String token ) {
        //
        //  REPLACE THE STATEMENT BELOW WITH YOUR CODE
        //
        int hash = hash_function(token);
        long ptr = get_pointer_from_hash(hash);
        String data[];

        while (true) {
            Entry e = readEntry(ptr);
            data = readData(e.start_ptr, (int)(e.end_ptr-e.start_ptr)).split(DATA_SEPARATOR);
            ptr = e.collision_ptr;
            
            if (data[0].equals(token))
                break;

            if (ptr == -1L)                
                return null; // token does not exsist

        }

        PostingsList p = PostingsList.deserialize(data);

        return p;
    }


    /**
     *  Inserts this token in the main-memory hashtable.
     */
    public void insert( String token, int docID, int offset ) {
        //
        //  YOUR CODE HERE
        //
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
        writeIndex(true);
        try {
            dataFile.getChannel().force(false);;
            dictionaryFile.getChannel().force(false);
            readDictionaryFile.getChannel().force(false);
            readDictionaryFile.close();
            dictionaryFile.close();
            dataFile.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.err.println( "done!" );
    }

    public static int hash_function(String in) {
        //return (int)Math.abs(Math.floor(in.hashCode()%TABLESIZE));
        /*int[] primes = {2719,21269,571699,1000423}; // {11,13,17,19,23,39,31,37,41,43};
        int num_primes = primes.length;
        double hash = 3;
        
        byte[] b = in.getBytes();
        for (int i = 0; i < b.length; i++) {
            hash *= (b[i]+0.5) * primes[i%num_primes];
        }

        return (int)Math.floor(Math.abs(hash%TABLESIZE));*/

        long hash = 0;
        for (int i = 0; i < in.length(); i++) {
            hash = 37*hash + in.charAt(i);
        }

        return (int)Math.abs(hash%TABLESIZE);
    }

    public static long get_pointer_from_hash(int hash) {
        return (Entry.byte_size+1) * hash; // adding one because I've never used java and dont know if some functions are inclusive and I dont feel like finding out
    }
}