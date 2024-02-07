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
import java.nio.channels.FileChannel;
import java.nio.charset.*;
import java.security.spec.ECFieldF2m;


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
    public static final long TABLESIZE = 3_000_017L; //3_503_119L //611953L

    /** The dictionary hash table is stored in this file. */
    RandomAccessFile dictionaryFile;

    /** The data (the PostingsLists) are stored in this file. */
    RandomAccessFile dataFile;

    /** Pointer to the first free memory cell in the data file. */
    long free = 0L;

    /** The cache as a main-memory hash map. */
    HashMap<String,PostingsList> index = new HashMap<String,PostingsList>();

    protected static final String DATA_SEPARATOR = " ";


    // ===================================================================

    /**
     *   A helper class representing one entry in the dictionary hashtable.
     */ 
    public class Entry {
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

    }

    /**
     *  Writes data to the data file at a specified place.
     *
     *  @return The number of bytes written.
     */ 
    int writeData( String dataString, long ptr ) {
        try {
            dataFile.seek( ptr ); 
            byte[] data = dataString.getBytes();
            dataFile.write( data );
            return data.length;
        } catch ( IOException e ) {
            e.printStackTrace();
            return -1;
        }
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
    void writeEntry(Entry entry, long ptr) {
        //
        //  YOUR CODE HERE
        //
        try {
            dictionaryFile.seek(ptr); 
            byte[] data = entry.get_bytes();
            dictionaryFile.write(data);
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
    Entry readEntry( long ptr ) {   
        //
        //  REPLACE THE STATEMENT BELOW WITH YOUR CODE 
        //
        try {
            dictionaryFile.seek( ptr );
            byte[] data = new byte[Entry.byte_size];
            dictionaryFile.readFully( data );
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

    /*protected ArrayList<String> IntersectionOfTerms(String[] terms1, String[] terms2) {
        Arrays.sort(terms1);
        Arrays.sort(terms2);
        int i = 0, j = 0;
        ArrayList<String> ret = new ArrayList<String>();

        while (i < terms1.length && j < terms2.length) {
            if (terms1[i].equals(terms2[j])) {
                ret.add(terms1[i]);
                i++; j++;
            } else if (terms1[i].compareTo(terms2[j]) < 0) {
                i++;
            } else {
                j++;
            }
        }

 

        if (ret.size() > 0) {
            return ret;
        }

        return null;
    }*/

    protected ArrayList<String> IntersectionOfTerms(String file1, String file2) {
        ArrayList<String> ret = new ArrayList<String>();
        try {
            File f1 = new File(file1);
            FileReader freader1 = new FileReader(f1);
            BufferedReader br1 = new BufferedReader(freader1);

            File f2 = new File(file2);
            FileReader freader2 = new FileReader(f2);
            BufferedReader br2 = new BufferedReader(freader2);

            String term1 = "", term2 = "";
            boolean readNextLine1 = true, readNextLine2 = true;
            while (true) {
                if (readNextLine1) {
                    term1 = br1.readLine();
                    readNextLine1 = false;
                    if (term1 == null) {
                        break;
                    }
                }
                if (readNextLine2) {
                    term2 = br2.readLine();
                    readNextLine2 = false;
                    if (term2 == null) {
                        break;
                    }
                }

                if (term1.equals(term2)) {
                    ret.add(term1);
                    readNextLine1 = true;
                    readNextLine2 = true;
                } else if (term1.compareTo(term2) < 0) {
                    readNextLine1 = true;
                } else {
                    readNextLine2 = true;
                }
            }

            freader1.close();
            freader2.close();
            br1.close();
            br2.close();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        if (ret.size() > 0) {
            return ret;
        }

        return null;
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
                    // Collision occured
                    collisions += 1;
                    // Get a new pointer to store the current postings list
                    int new_hash = find_first_collision_free(hashes_used);
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
        } catch ( IOException e ) {
            e.printStackTrace();
        }
        System.err.println( collisions + " collisions." );
    }

    protected class EndOfListResponse {
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

    protected int find_first_collision_free(int[] arr) {
        for (int i = 0; i < arr.length; i++) {
            if (arr[i] == 0){
                return i;
            }
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
        System.err.println( "done!" );
    }

    protected int hash_function(String in) {
        int[] primes = {11,13,17,19}; // {11,13,17,19,23,39,31,37,41,43};
        int num_primes = primes.length;
        double hash = 3;
        
        byte[] b = in.getBytes();
        for (int i = 0; i < b.length; i++) {
            hash *= (b[i]+0.5) * primes[i%num_primes];
        }

        return (int)Math.floor(Math.abs(hash%TABLESIZE));
        //return (int)Math.abs(Math.floor(in.hashCode()%TABLESIZE));
    }

    protected long get_pointer_from_hash(int hash) {
        return (Entry.byte_size+1) * hash; // adding one because I've never used java and dont know if some functions are inclusive and I dont feel like finding out
    }
}
