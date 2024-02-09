package ir;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;

import ir.PersistentHashedIndex.EndOfListResponse;
import ir.PersistentHashedIndex.writeBuffer;
import ir.PersistentHashedIndex.Entry;

public class Merger {
    private String merge_data_location;
    private String merge_dict_location;
    private String merge_terms_location;
    private String merge_docinfo;
    private String main_data_location;
    private String main_dict_location; 
    private String main_terms_location;
    private String main_docinfo;
    private String new_append;

    public Merger(String[] args) {
        decodeArgs(args);
        merge_files(main_data_location, main_dict_location, main_terms_location, main_docinfo,
                        merge_data_location, merge_dict_location, merge_terms_location, merge_docinfo,
                        new_append);
    }

    private void decodeArgs( String[] args ) {
        this.main_data_location     = args[0];
        this.main_dict_location     = args[1];
        this.main_terms_location    = args[2];
        this.main_docinfo           = args[3];
        this.merge_data_location    = args[4];
        this.merge_dict_location    = args[5];
        this.merge_terms_location   = args[6];
        this.merge_docinfo          = args[7];
        this.new_append             = args[8];            
    }

    public static void main( String[] args ) {
        Merger m = new Merger( args );
    }

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
            tempData = new RandomAccessFile(PersistentHashedIndex.INDEXDIR + "/" + PersistentHashedIndex.DATA_FNAME + new_append + "_merge", "rw" );
            tempDict = new RandomAccessFile(PersistentHashedIndex.INDEXDIR + "/" + PersistentHashedIndex.DICTIONARY_FNAME + new_append + "_merge", "rw" );
            tempReadDict = new RandomAccessFile(PersistentHashedIndex.INDEXDIR + "/" + PersistentHashedIndex.DICTIONARY_FNAME + new_append + "_merge", "r" );
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
        int[] hashes_used = new int[(int)PersistentHashedIndex.TABLESIZE];
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
                    write_data = String.join(PersistentHashedIndex.DATA_SEPARATOR, main_sdata);
                }
                
                // Write the data to the datafile
                int written_data = writeData(tempData, write_data, free_ptr);
                // Save the starting pointer and ending pointer
                Entry e = new Entry(free_ptr, free_ptr+written_data); // this is the postings list for token "key" 
                // Increment the starting pointer
                free_ptr += written_data; // +1 ?
                // Get hash of token
                int hash = PersistentHashedIndex.hash_function(token);
                // Get the pointer corresponding to the location in the dictionary
                long ptr = PersistentHashedIndex.get_pointer_from_hash(hash);
                
                if (hashes_used[hash] == 0) {
                    //writeEntry(tempDict, e, ptr);
                    dictBuffer.add(new writeBuffer(e.get_bytes(), ptr));
                    hashes_used[hash] = 1;
                } else {
                    emptyDictBuffer(dictBuffer, tempDict);
                    dictBuffer.clear();
                    // Get a new pointer to store the current postings list
                    int new_hash = PersistentHashedIndex.find_first_collision_free(hashes_used, hash);
                    long new_ptr = PersistentHashedIndex.get_pointer_from_hash(new_hash);
                    
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
                String write_data = String.join(PersistentHashedIndex.DATA_SEPARATOR, merge_sdata);
                // Write the data to the datafile
                int written_data = writeData(tempData, write_data, free_ptr);
                // Save the starting pointer and ending pointer
                Entry e = new Entry(free_ptr, free_ptr+written_data); // this is the postings list for token "key" 
                // Increment the starting pointer
                free_ptr += written_data; // +1 ?
                // Get hash of token
                int hash = PersistentHashedIndex.hash_function(token);
                // Get the pointer corresponding to the location in the dictionary
                long ptr = PersistentHashedIndex.get_pointer_from_hash(hash);
                
                if (hashes_used[hash] == 0) {
                    //writeEntry(tempDict, e, ptr);
                    dictBuffer.add(new writeBuffer(e.get_bytes(), ptr));
                    hashes_used[hash] = 1;
                } else {
                    emptyDictBuffer(dictBuffer, tempDict);
                    dictBuffer.clear();
                    // Get a new pointer to store the current postings list
                    int new_hash = PersistentHashedIndex.find_first_collision_free(hashes_used, hash);
                    long new_ptr = PersistentHashedIndex.get_pointer_from_hash(new_hash);
                    
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

        mergeTerms(main_terms_location, merge_terms_location, PersistentHashedIndex.INDEXDIR + "/" + PersistentHashedIndex.TERMS_FNAME + new_append);
        mergeDocInfo(main_docinfo, merge_docinfo, PersistentHashedIndex.INDEXDIR + "/" + PersistentHashedIndex.DOCINFO_FNAME + new_append);

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

            Path f9 = Paths.get(PersistentHashedIndex.INDEXDIR + "/" + PersistentHashedIndex.DATA_FNAME + new_append + "_merge");
            Path f10 = Paths.get(PersistentHashedIndex.INDEXDIR + "/" + PersistentHashedIndex.DICTIONARY_FNAME + new_append + "_merge");
            Path f11 = Paths.get(PersistentHashedIndex.INDEXDIR + "/" + PersistentHashedIndex.DATA_FNAME + new_append);
            Path f12 = Paths.get(PersistentHashedIndex.INDEXDIR + "/" + PersistentHashedIndex.DICTIONARY_FNAME + new_append);

            Files.move(f9, f11);
            Files.move(f10, f12);
        } catch ( Exception e ) {
            e.printStackTrace();
        }
        
        //created_indicies.add(new_append);
        System.err.println("Finished merge: " + new_append + ", took: " + (System.currentTimeMillis() - start_time)/1000L + " seconds");
    }

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

    private String[] get_positings_data(RandomAccessFile data, RandomAccessFile dict, String token) {
        int hash = PersistentHashedIndex.hash_function(token);
        long ptr = PersistentHashedIndex.get_pointer_from_hash(hash);
        String ret_data[];

        while (true) {
            Entry e = readEntry(dict, ptr);
            ret_data = readData(data, e.start_ptr, (int)(e.end_ptr-e.start_ptr)).split(PersistentHashedIndex.DATA_SEPARATOR);
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
                ret += PersistentHashedIndex.DATA_SEPARATOR + resp.data;
                i = resp.ptr;
            } else {
                Idisslikejava resp = condenseData(merge_data, j);
                ret += PersistentHashedIndex.DATA_SEPARATOR + resp.data;
                j = resp.ptr;
            }
        }

        while (i < main_data.length) {
            ret += PersistentHashedIndex.DATA_SEPARATOR + main_data[i++];
        }
        
        while (j < merge_data.length) {
            ret += PersistentHashedIndex.DATA_SEPARATOR + merge_data[j++];
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
        ret = docID + PersistentHashedIndex.DATA_SEPARATOR + score + PersistentHashedIndex.DATA_SEPARATOR + number_of_offsets;
        for (int j = 0; j < number_of_offsets; j++) {
            String offset = data[i++];
            ret += PersistentHashedIndex.DATA_SEPARATOR + offset;
        }

        return new Idisslikejava(ret, i);
    }

    private EndOfListResponse find_end_of_list(RandomAccessFile file, long ptr) {
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
}
