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
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
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

    public static final int BATCHSIZE = 1_000_000; //3_000_000;//10_000_000;

    private ArrayList<Thread> created_threads = new ArrayList<Thread>(); // Store which files each thread is working on

    private ArrayList<String> created_indicies = new ArrayList<String>(Arrays.asList("")); // Stores whatever is appended to the file name

    private String current_extension = "";
    
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

            //ByteBuffer bb = file.getChannel().map(FileChannel.MapMode.READ_WRITE, ptr, data.length);
            //bb.put(data);
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
    /*void writeEntry(RandomAccessFile file, Entry entry, long ptr) {
        try {
            file.seek(ptr); 
            byte[] data = entry.get_bytes();
            file.write(data);
            return;
        } catch ( IOException e ) {
            e.printStackTrace();
            return;
        }
    }*/
    void emptyDictBuffer(ArrayList<writeBuffer> buf, RandomAccessFile file) {
        try {
            Collections.sort(buf, (a,b)-> (int)(a.ptr-b.ptr));
            for (writeBuffer b : buf) {
                file.seek( b.ptr ); 
                file.write( b.data );
            }
        } catch ( IOException e ) {
            e.printStackTrace();
        }
    }

    /**
     *  Reads an entry from the dictionary file.
     *
     *  @param ptr The place in the dictionary file where to start reading.
     */
    Entry readEntry(RandomAccessFile file, long ptr) {   
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
    private int lastDocID = -1;
    private long tokensWritten = 0L;
    public void insert( String token, int docID, int offset ) {
        if (tokensWritten >= BATCHSIZE && lastDocID != docID) {
            write_intermediate(true);
            tokensWritten = 0L;
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
        tokensWritten++;
    }

    private void write_intermediate(boolean new_file) {
        System.err.println("Writing intermediate!");
        writeTerms(index.keySet().toArray(new String[index.size()]), INDEXDIR + "/" + TERMS_FNAME + current_extension);
        writeIndex(false);
        try {
            writeDocInfo(INDEXDIR + "/" + DOCINFO_FNAME + current_extension);
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            dataFile.getChannel().force(true);
            dictionaryFile.getChannel().force(true);
            readDictionaryFile.getChannel().force(true);
            readDictionaryFile.close();
            dictionaryFile.close();
            dataFile.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Make a new files and reset
        if (current_extension.equals(""))
            current_extension = "0";
        
        current_extension = Integer.toString(Integer.parseInt(current_extension)+1);
        String new_data_location = INDEXDIR + "/" + DATA_FNAME + current_extension;
        String new_dict_location = INDEXDIR + "/" + DICTIONARY_FNAME + current_extension;
        //String new_terms_location = INDEXDIR + "/" + TERMS_FNAME + created_indicies.size();

        free = 0L;
        index = new HashMap<String, PostingsList>(); //.clear();
        docLengths.clear();
        docNames.clear();
        if (new_file){
            System.err.println("New file: " + new_data_location);
            try {
                dictionaryFile = new RandomAccessFile(new_dict_location, "rw" );
                readDictionaryFile = new RandomAccessFile(new_dict_location, "r");
                dataFile = new RandomAccessFile(new_data_location, "rw" );
            } catch ( IOException e ) {
                e.printStackTrace();
            }
        }

        if (created_indicies.size() > 1) {
            String add1 = created_indicies.remove(0);
            String add2 = created_indicies.remove(0);

            if (add1.equals(add2))
                System.err.println("Wtf");

            String main_data = INDEXDIR + "/" + DATA_FNAME + add1;
            String main_dict = INDEXDIR + "/" + DICTIONARY_FNAME + add1;
            String main_terms = INDEXDIR + "/" + TERMS_FNAME + add1;
            String main_docinfo = INDEXDIR + "/" + DOCINFO_FNAME + add1;
            String merge_data = INDEXDIR + "/" + DATA_FNAME + add2;
            String merge_dict = INDEXDIR + "/" + DICTIONARY_FNAME + add2;
            String merge_terms = INDEXDIR + "/" + TERMS_FNAME + add2;
            String merge_docinfo = INDEXDIR + "/" + DOCINFO_FNAME + add2;

            Merger m = new Merger(main_data, main_dict, main_terms, main_docinfo, merge_data, merge_dict, merge_terms, merge_docinfo, Long.valueOf(System.currentTimeMillis()) + "");
            Thread thread = new Thread(m);
            created_threads.add(thread);
            thread.start();
        }

        if (new_file)
            created_indicies.add(current_extension);
    }

    /**
     *  Write index to file after indexing is done.
     */
    @Override
    public void cleanup() {
        System.err.println("Finished indexing." );
        System.err.print( "Waiting for merges to complete.. ");

        write_intermediate(false);

        try {
            while (created_indicies.size() != 1 || created_threads.size() > 0) {
                Thread.sleep(500);
                ArrayList<Thread> rmv = new ArrayList<Thread>();
                for (Thread t : created_threads) {
                    if (t.getState() == Thread.State.TERMINATED)
                        rmv.add(t);
                }
                created_threads.removeAll(rmv);

                if (created_indicies.size() > 1) {
                    String add1 = created_indicies.remove(0);
                    String add2 = created_indicies.remove(0);
        
                    if (add1.equals(add2))
                        System.err.println("Wtf");
        
                    String main_data = INDEXDIR + "/" + DATA_FNAME + add1;
                    String main_dict = INDEXDIR + "/" + DICTIONARY_FNAME + add1;
                    String main_terms = INDEXDIR + "/" + TERMS_FNAME + add1;
                    String main_docinfo = INDEXDIR + "/" + DOCINFO_FNAME + add1;
                    String merge_data = INDEXDIR + "/" + DATA_FNAME + add2;
                    String merge_dict = INDEXDIR + "/" + DICTIONARY_FNAME + add2;
                    String merge_terms = INDEXDIR + "/" + TERMS_FNAME + add2;
                    String merge_docinfo = INDEXDIR + "/" + DOCINFO_FNAME + add2;
        
                    Merger m = new Merger(main_data, main_dict, main_terms, main_docinfo, merge_data, merge_dict, merge_terms, merge_docinfo, Long.valueOf(System.currentTimeMillis()) + "");
                    Thread thread = new Thread(m);
                    created_threads.add(thread);
                    thread.start();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        try {
            File f1 = new File(INDEXDIR + "/" + DATA_FNAME + created_indicies.get(0));
            File f2 = new File(INDEXDIR + "/" + DICTIONARY_FNAME + created_indicies.get(0));
            File f3 = new File(INDEXDIR + "/" + TERMS_FNAME + created_indicies.get(0));
            File f4 = new File(INDEXDIR + "/" + DOCINFO_FNAME + created_indicies.get(0));

            f1.renameTo(new File(INDEXDIR + "/" + DATA_FNAME));
            f2.renameTo(new File(INDEXDIR + "/" + DICTIONARY_FNAME));
            f3.renameTo(new File(INDEXDIR + "/" + TERMS_FNAME));
            f4.renameTo(new File(INDEXDIR + "/" + DOCINFO_FNAME));
            
            dataFile = new RandomAccessFile(INDEXDIR + "/" + DATA_FNAME, "rw");
            dictionaryFile = new RandomAccessFile(INDEXDIR + "/" + DICTIONARY_FNAME, "rw");
            readDictionaryFile = new RandomAccessFile(INDEXDIR + "/" + DICTIONARY_FNAME, "r");
            readDocInfo();
        } catch ( IOException e ) {
            e.printStackTrace();
        }

        System.err.println( "Last done! " + LocalDateTime.now());
    }

    private void merge_files(String main_data_location, String main_dict_location, String main_terms_location, String main_docinfo,
                             String merge_data_location, String merge_dict_location, String merge_terms_location, String merge_docinfo,
                             String new_append) {
        
        System.err.println("Started merge: " + new_append);
        long start_time = System.currentTimeMillis();

        RandomAccessFile tempData = null;
        RandomAccessFile tempDict = null;
        RandomAccessFile tempReadDict = null;
        RandomAccessFile main_data = null;
        RandomAccessFile main_dict = null;
        RandomAccessFile merge_data = null;
        RandomAccessFile merge_dict = null;

        try {
            tempData = new RandomAccessFile(INDEXDIR + "/" + DATA_FNAME + new_append + "_merge", "rw" );
            tempDict = new RandomAccessFile(INDEXDIR + "/" + DICTIONARY_FNAME + new_append + "_merge", "rw" );
            tempReadDict = new RandomAccessFile(INDEXDIR + "/" + DICTIONARY_FNAME + new_append + "_merge", "r" );
            main_data = new RandomAccessFile(main_data_location, "rw" );
            main_dict = new RandomAccessFile(main_dict_location, "rw" );
            merge_data = new RandomAccessFile(merge_data_location, "rw" );
            merge_dict = new RandomAccessFile(merge_dict_location, "rw" );
        } catch ( Exception e ) {
            e.printStackTrace();
        }

        long free_ptr = 0;

        ArrayList<String> tokensToBeMerged = IntersectionOfTerms(main_terms_location, merge_terms_location);
        
        // Rewrite the datafile, rewrite entry if postingslist needs to be merged
        int[] hashes_used = new int[(int)TABLESIZE];
        try {
            File main_terms = new File(main_terms_location);
            FileReader freader = new FileReader(main_terms);
            BufferedReader br = new BufferedReader(freader);
            String token, write_data = "";
            ArrayList<writeBuffer> dictBuffer = new ArrayList<writeBuffer>();
            String[] main_sdata;
            String[] merge_sdata;
            while ((token = br.readLine()) != null) {
                main_sdata = get_positings_data(main_data, main_dict, token);
                merge_sdata = null;

                if (tokensToBeMerged.contains(token)) {
                    // merge 
                    merge_sdata = get_positings_data(merge_data, merge_dict, token);
                    write_data = mergeData(main_sdata, merge_sdata);
                } else {
                    write_data = String.join(DATA_SEPARATOR, main_sdata);
                }
                
                // Write the data to the datafile
                int written_data = writeData(tempData, write_data, free_ptr);
                // Save the starting pointer and ending pointer
                Entry e = new Entry(free_ptr, free_ptr+written_data); // this is the postings list for token "key" 
                // Increment the starting pointer
                free_ptr += written_data; // +1 ?
                // Get hash of token
                int hash = hash_function(token);
                // Get the pointer corresponding to the location in the dictionary
                long ptr = get_pointer_from_hash(hash);
                
                if (hashes_used[hash] == 0) {
                    //writeEntry(tempDict, e, ptr);
                    dictBuffer.add(new writeBuffer(e.get_bytes(), ptr));
                    hashes_used[hash] = 1;
                } else {
                    emptyDictBuffer(dictBuffer, tempDict);
                    dictBuffer.clear();
                    // Get a new pointer to store the current postings list
                    int new_hash = find_first_collision_free(hashes_used, hash);
                    long new_ptr = get_pointer_from_hash(new_hash);
                    
                    // Get the entry at the collision hash value
                    EndOfListResponse response = find_end_of_list(tempReadDict, ptr);
                    // Save the new pointer to the collision entry
                    Entry col_entry = response.entry;
                    col_entry.collision_ptr = new_ptr;
                    //writeEntry(tempDict, col_entry, response.ptr);
                    dictBuffer.add(new writeBuffer(col_entry.get_bytes(), response.ptr));
                    
                    // Write the entry to its new position
                    //writeEntry(tempDict, e, new_ptr);
                    dictBuffer.add(new writeBuffer(e.get_bytes(), new_ptr));
                    hashes_used[new_hash] = 1;
                }
            }
            emptyDictBuffer(dictBuffer, tempDict);
            freader.close();
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            File merge_terms = new File(merge_terms_location);
            FileReader freader = new FileReader(merge_terms);
            BufferedReader br = new BufferedReader(freader);
            String token;
            ArrayList<writeBuffer> dictBuffer = new ArrayList<writeBuffer>();
            while ((token = br.readLine()) != null) {
                if (tokensToBeMerged.contains(token)) {
                    continue;
                }
                String merge_sdata[] = get_positings_data(merge_data, merge_dict, token);
                String write_data = String.join(DATA_SEPARATOR, merge_sdata);
                // Write the data to the datafile
                int written_data = writeData(tempData, write_data, free_ptr);
                // Save the starting pointer and ending pointer
                Entry e = new Entry(free_ptr, free_ptr+written_data); // this is the postings list for token "key" 
                // Increment the starting pointer
                free_ptr += written_data; // +1 ?
                // Get hash of token
                int hash = hash_function(token);
                // Get the pointer corresponding to the location in the dictionary
                long ptr = get_pointer_from_hash(hash);
                
                if (hashes_used[hash] == 0) {
                    //writeEntry(tempDict, e, ptr);
                    dictBuffer.add(new writeBuffer(e.get_bytes(), ptr));
                    hashes_used[hash] = 1;
                } else {
                    emptyDictBuffer(dictBuffer, tempDict);
                    dictBuffer.clear();
                    // Get a new pointer to store the current postings list
                    int new_hash = find_first_collision_free(hashes_used, hash);
                    long new_ptr = get_pointer_from_hash(new_hash);
                    
                    // Get the entry at the collision hash value
                    EndOfListResponse response = find_end_of_list(tempReadDict, ptr);
                    // Save the new pointer to the collision entry
                    Entry col_entry = response.entry;
                    col_entry.collision_ptr = new_ptr;
                    //writeEntry(tempDict, col_entry, response.ptr);
                    dictBuffer.add(new writeBuffer(col_entry.get_bytes(), response.ptr));
                    
                    // Write the entry to its new position
                    //writeEntry(tempDict, e, new_ptr);
                    dictBuffer.add(new writeBuffer(e.get_bytes(), new_ptr));
                    hashes_used[new_hash] = 1;
                }
            }
            emptyDictBuffer(dictBuffer, tempDict);
            freader.close();
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        mergeTerms(main_terms_location, merge_terms_location, INDEXDIR + "/" + TERMS_FNAME + new_append);
        mergeDocInfo(main_docinfo, merge_docinfo, INDEXDIR + "/" + DOCINFO_FNAME + new_append);

        // Delete files and rename temp
        try {
            tempData.getChannel().force(true);
            tempDict.getChannel().force(true);
            tempReadDict.getChannel().force(true);

            tempData.close();
            tempDict.close();
            tempReadDict.close();
            main_data.close();
            main_dict.close();
            merge_data.close();
            merge_dict.close();

            File f1 = new File(main_data_location);
            File f2 = new File(main_dict_location);
            File f3 = new File(main_terms_location);
            File f4 = new File(merge_data_location);
            File f5 = new File(merge_dict_location);
            File f6 = new File(merge_terms_location);
            File f7 = new File(main_docinfo);
            File f8 = new File(merge_docinfo);

            f1.delete();
            f2.delete();
            f3.delete();
            f4.delete();
            f5.delete();
            f6.delete();
            f7.delete();
            f8.delete();

            Path f9 = Paths.get(INDEXDIR + "/" + DATA_FNAME + new_append + "_merge");
            Path f10 = Paths.get(INDEXDIR + "/" + DICTIONARY_FNAME + new_append + "_merge");
            Path f11 = Paths.get(INDEXDIR + "/" + DATA_FNAME + new_append);
            Path f12 = Paths.get(INDEXDIR + "/" + DICTIONARY_FNAME + new_append);

            Files.move(f9, f11);
            Files.move(f10, f12);
        } catch ( Exception e ) {
            e.printStackTrace();
        }
        
        created_indicies.add(new_append);
        System.err.println("Finished merge: " + new_append + ", took: " + (System.currentTimeMillis() - start_time)/1000L + " seconds");

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
            if (e.collision_ptr == -1L) {
                break; // found the end of the linked list
            } else {
                ptr = e.collision_ptr;
            }
        }

        return new EndOfListResponse(e, ptr);
    }

    private String[] get_positings_data(RandomAccessFile data, RandomAccessFile dict, String token) {
        int hash = hash_function(token);
        long ptr = get_pointer_from_hash(hash);
        String ret_data[];

        while (true) {
            Entry e = readEntry(dict, ptr);
            ret_data = readData(data, e.start_ptr, (int)(e.end_ptr-e.start_ptr)).split(DATA_SEPARATOR);
            ptr = e.collision_ptr;
            
            if (ret_data[0].equals(token))
                break;

            if (e.collision_ptr == -1L) {
                System.err.println("Null!!!");
                System.err.println(token);
            }

            if (ptr == -1L)                
                return null; // token does not exsist
        }

        return ret_data;
    }

    private void mergeTerms(String main_terms, String merge_terms, String outputFile) {
        try {
            FileOutputStream fout = new FileOutputStream(outputFile);
            File fmain = new File(main_terms);
            File fmerge = new File(merge_terms);
            FileReader frmain = new FileReader(fmain);
            BufferedReader brmain = new BufferedReader(frmain);
            FileReader frmerge = new FileReader(fmerge);
            BufferedReader brmerge = new BufferedReader(frmerge);
            String line1 = brmain.readLine();
            String line2 = brmerge.readLine();
            while (line1 != null && line2 != null) {
                if (line1.equals(line2)) {
                    fout.write((line1+"\n").getBytes());
                    line1 = brmain.readLine();
                    line2 = brmerge.readLine();
                } else if (line1.compareTo(line2) < 0) {
                    fout.write((line1+"\n").getBytes());
                    line1 = brmain.readLine();
                } else {
                    fout.write((line2+"\n").getBytes());
                    line2 = brmerge.readLine();
                }
            }

            while (line1 != null) {
                fout.write((line1+"\n").getBytes());
                line1 = brmain.readLine();
            }
            
            while (line2 != null) {
                fout.write((line2+"\n").getBytes());
                line2 = brmerge.readLine();
            }

            fout.close();
            frmain.close();
            brmain.close();
            frmerge.close();
            brmerge.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void mergeDocInfo(String main_terms, String merge_terms, String outputFile) {
        try {
            FileOutputStream fout = new FileOutputStream(outputFile);
            File fmain = new File(main_terms);
            File fmerge = new File(merge_terms);
            FileReader frmain = new FileReader(fmain);
            BufferedReader brmain = new BufferedReader(frmain);
            FileReader frmerge = new FileReader(fmerge);
            BufferedReader brmerge = new BufferedReader(frmerge);
            String line1 = brmain.readLine();
            String line2 = brmerge.readLine();
            String[] data1 = line1.split(";");
            String[] data2 = line2.split(";");
            while (line1 != null && line2 != null) {
                int i = Integer.parseInt(data1[0]);
                int j = Integer.parseInt(data2[0]);
                if (i == j) {
                    fout.write((line1+"\n").getBytes());
                    line1 = brmain.readLine();
                    line2 = brmerge.readLine();
                    if (line1 != null && line2 != null) {
                        data1 = line1.split(";");
                        data2 = line2.split(";");
                    }
                } else if (i < j) {
                    fout.write((line1+"\n").getBytes());
                    line1 = brmain.readLine();
                    if (line1 != null)
                        data1 = line1.split(";");
                } else {
                    fout.write((line2+"\n").getBytes());
                    line2 = brmerge.readLine();
                    if (line2 != null)
                        data2 = line2.split(";");
                }
            }

            while (line1 != null) {
                fout.write((line1+"\n").getBytes());
                line1 = brmain.readLine();
            }
            
            while (line2 != null) {
                fout.write((line2+"\n").getBytes());
                line2 = brmerge.readLine();
            }

            fout.close();
            frmain.close();
            brmain.close();
            frmerge.close();
            brmerge.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /*private String mergeData(String[] main_data, String[] merge_data) {
        String write_data = String.join(DATA_SEPARATOR, main_data);
        merge_data[0] = "";
        write_data += String.join(DATA_SEPARATOR, merge_data);

        // Right now PostingsList.deserialize sorts the deserialized postingslist.
        // Should change it so that all the docID's are written in order 

        return write_data;
    }*/

    private String mergeData(String[] main_data, String[] merge_data) {
        String ret = main_data[0];  
        int i = 1, j = 1;          
        while (i < main_data.length && j < merge_data.length) {
            int mainDocID = Integer.parseInt(main_data[i]);
            int mergeDocID = Integer.parseInt(merge_data[j]);
            if (mainDocID == mergeDocID) {
                System.err.println("This should not happen!!=?!==!=!=?!?!??!?!??!??!!!!");
            } else if (mainDocID < mergeDocID) {
                Idisslikejava resp = condenseData(main_data, i);
                ret += DATA_SEPARATOR + resp.data;
                i = resp.ptr;
            } else {
                Idisslikejava resp = condenseData(merge_data, j);
                ret += DATA_SEPARATOR + resp.data;
                j = resp.ptr;
            }
        }

        while (i < main_data.length) {
            ret += DATA_SEPARATOR + main_data[i++];
        }
        
        while (j < merge_data.length) {
            ret += DATA_SEPARATOR + merge_data[j++];
        }

        return ret;
    }

    public class Idisslikejava {
        public String data;
        public int ptr;
        public Idisslikejava(String d, int p) {
            this.data = d;
            this.ptr = p;
        }
    }
    private Idisslikejava condenseData(String[] data, int i) {
        String ret = "";
        String docID = data[i++];
        String score = data[i++];
        int number_of_offsets = Integer.valueOf(data[i++]);
        ret = docID + DATA_SEPARATOR + score + DATA_SEPARATOR + number_of_offsets;
        for (int j = 0; j < number_of_offsets; j++) {
            String offset = data[i++];
            ret += DATA_SEPARATOR + offset;
        }

        return new Idisslikejava(ret, i);
    }

    public class Merger extends Thread {
        private String merge_data_location;
        private String merge_dict_location;
        private String merge_terms_location;
        private String merge_docinfo;
        private String main_data_location;
        private String main_dict_location; 
        private String main_terms_location;
        private String main_docinfo;
        private String new_append;
        public Merger(String main_data_location, String main_dict_location, String main_terms_location, String main_docinfo,
                      String merge_data_location, String merge_dict_location, String merge_terms_location, String merge_docinfo,
                      String new_append) {
            super();
            this.main_data_location = main_data_location;
            this.main_dict_location = main_dict_location;
            this.main_terms_location = main_terms_location;
            this.main_docinfo = main_docinfo;
            this.merge_data_location = merge_data_location;
            this.merge_dict_location = merge_dict_location;
            this.merge_terms_location = merge_terms_location;
            this.merge_docinfo = merge_docinfo;
            this.new_append = new_append;
        }

        public void run() {
            merge_files(main_data_location, main_dict_location, main_terms_location, main_docinfo,
                        merge_data_location, merge_dict_location, merge_terms_location, merge_docinfo,
                        new_append);
                
        }
    }
}
