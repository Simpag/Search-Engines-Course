/*  
 *   This file is part of the computer assignment for the
 *   Information Retrieval course at KTH.
 * 
 *   Johan Boye, KTH, 2018
 */  

package ir;

import java.io.*;
import java.util.*;
import java.time.Duration;
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
public class PersistentScalableHashedIndex extends PersistentHashedIndex {

    public static final int BATCHSIZE = 1_000_000; //3_000_000;//10_000_000;

    private ArrayList<RunningProcess> created_processes = new ArrayList<RunningProcess>(); // Store which files each thread is working on

    private ArrayList<String> created_indicies = new ArrayList<String>(Arrays.asList("")); // Stores whatever is appended to the file name

    private String current_extension = "";

    public class RunningProcess {
        private Process process;
        private String extension;
        private long startTime;

        public RunningProcess(Process p, String e, long time) {
            process = p;
            extension = e;
            startTime = time;
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

        checkProcesses();
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
            String extension = Long.valueOf(System.currentTimeMillis()) + "";

            //Merger m = new Merger(main_data, main_dict, main_terms, main_docinfo, merge_data, merge_dict, merge_terms, merge_docinfo, Long.valueOf(System.currentTimeMillis()) + "");
            //Thread thread = new Thread(m);
            //created_threads.add(thread);
            //thread.start();
            try {
                ProcessBuilder builder = new ProcessBuilder("java", "-cp", "classes", "-Xmx1g", "ir.Merger", main_data, main_dict, main_terms, main_docinfo, merge_data, merge_dict, merge_terms, merge_docinfo, extension);
                builder.directory(new File("."));
                Process process = builder.start();
                RunningProcess rp = new RunningProcess(process, extension, System.currentTimeMillis());
                created_processes.add(rp);
            } catch (Exception e) {
                e.printStackTrace();
            }
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
            while (created_indicies.size() != 1 || created_processes.size() > 0) {
                Thread.sleep(500);
                checkProcesses();

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
                    String extension = Long.valueOf(System.currentTimeMillis()) + "";
        
                    try {
                        ProcessBuilder builder = new ProcessBuilder("java", "-cp", "classes", "-Xmx1g", "ir.Merger", main_data, main_dict, main_terms, main_docinfo, merge_data, merge_dict, merge_terms, merge_docinfo, extension);
                        builder.directory(new File("."));
                        Process process = builder.start();
                        RunningProcess rp = new RunningProcess(process, extension, System.currentTimeMillis());
                        created_processes.add(rp);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
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

        System.err.println( "Completed in: " + Duration.between(startTime, LocalDateTime.now()).toMillis() / 1000 + " seconds");
    }

    private void checkProcesses() {
        ArrayList<RunningProcess> rmv = new ArrayList<RunningProcess>();
        for (RunningProcess p : created_processes) {
            if (!p.process.isAlive()) {
                rmv.add(p);
                created_indicies.add(p.extension);
                System.err.println("Merge: " + p.extension + " finished in: " + (System.currentTimeMillis() - p.startTime)/1000L + " seconds");
            }
        }

        created_processes.removeAll(rmv);
        if (rmv.size() > 0 )
            System.err.println("Removed: " + rmv.size() + ", now: " + created_processes.size());
    }
}
