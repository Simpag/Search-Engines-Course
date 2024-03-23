/*  
 *   This file is part of the computer assignment for the
 *   Information Retrieval course at KTH.
 * 
 *   Johan Boye, 2017
 */  

package ir;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.StringTokenizer;
import java.util.Iterator;
import java.util.Set;
import java.nio.charset.*;
import java.io.*;


/**
 *  A class for representing a query as a list of words, each of which has
 *  an associated weight.
 */
public class Query {

    /**
     *  Help class to represent one query term, with its associated weight. 
     */
    class QueryTerm {
        String term;
        double weight;
        QueryTerm( String t, double w ) {
            term = t;
            weight = w;
        }
    }

    /** 
     *  Representation of the query as a list of terms with associated weights.
     *  In assignments 1 and 2, the weight of each term will always be 1.
     */
    public ArrayList<QueryTerm> queryterm = new ArrayList<QueryTerm>();

    /**  
     *  Relevance feedback constant alpha (= weight of original query terms). 
     *  Should be between 0 and 1.
     *  (only used in assignment 3).
     */
    double alpha = 0.99;

    /**  
     *  Relevance feedback constant beta (= weight of query terms obtained by
     *  feedback from the user). 
     *  (only used in assignment 3).
     */
    double beta = 1 - alpha;
    
    
    /**
     *  Creates a new empty Query 
     */
    public Query() {
    }
    
    
    /**
     *  Creates a new Query from a string of words
     */
    public Query( String queryString  ) {
        StringTokenizer tok = new StringTokenizer( queryString );
        while ( tok.hasMoreTokens() ) {
            queryterm.add( new QueryTerm(tok.nextToken(), 1.0) );
        }    
    }
    
    
    /**
     *  Returns the number of terms
     */
    public int size() {
        return queryterm.size();
    }
    
    
    /**
     *  Returns the Manhattan query length
     */
    public double length() {
        double len = 0;
        for ( QueryTerm t : queryterm ) {
            len += t.weight; 
        }
        return len;
    }
    
    
    /**
     *  Returns a copy of the Query
     */
    public Query copy() {
        Query queryCopy = new Query();
        for ( QueryTerm t : queryterm ) {
            queryCopy.queryterm.add( new QueryTerm(t.term, t.weight) );
        }
        return queryCopy;
    }
    
    
    /**
     *  Expands the Query using Relevance Feedback
     *
     *  @param results The results of the previous query.
     *  @param docIsRelevant A boolean array representing which query results the user deemed relevant.
     *  @param engine The search engine object
     */
    public void relevanceFeedback( PostingsList results, boolean[] docIsRelevant, Engine engine ) {
        //
        //  YOUR CODE HERE
        //
        long startTime = System.currentTimeMillis();

        Set<String> allTerms = new HashSet<String>();
        int num_relevant = 0;

        for (int i = 0; i < docIsRelevant.length; i++) {
            if (docIsRelevant[i]) {
                num_relevant++;
            }
        }

        for (QueryTerm qt : queryterm) {
            qt.weight *= alpha;
            allTerms.add(qt.term);
        }

        for (int i = 0; i < docIsRelevant.length; i++) {
            if (!docIsRelevant[i])
                continue;

            String docName = Index.docNames.get(results.get(i).docID);
            ArrayList<String> tokens = getTokensInDoc(docName, engine);
            Set<String> uniqueTokens = new HashSet<String>(tokens);
            
            for (String token : uniqueTokens) {
                double weight = beta * Collections.frequency(tokens, token) / num_relevant;

                if (allTerms.contains(token)) {
                    int index = findIndexOfTerm(token);
                    assert queryterm.get(index).term.equals(token) : "Something went wrong..";
                    queryterm.get(index).weight += weight;
                } else {
                    allTerms.add(token);
                    queryterm.add( new QueryTerm(token, weight) );
                }
            }            
        }

        // long elapsedTime = System.currentTimeMillis() - startTime;
        // System.err.println("Feedback took: " + elapsedTime + " ms");


        // 1. Get the relevant documents
        // 2. Add the terms in those documents onto the queryterm list
        // 3. 
    }

    private ArrayList<String> getTokensInDoc(String filename, Engine engine) {
        ArrayList<String> tokens = new ArrayList<String>();
        try {
            Reader reader = new InputStreamReader( new FileInputStream(filename), StandardCharsets.UTF_8 );
            Tokenizer tok = new Tokenizer( reader, true, false, true, engine.indexer.patterns_file );
            while ( tok.hasMoreTokens() ) {
                String token = tok.nextToken();
                tokens.add(token);
            }
            reader.close();
        } catch ( IOException e ) {
            System.err.println( "Warning: IOException during indexing." );
        }

        return tokens;
    }

    private int findIndexOfTerm(String term) {
        for (int i = 0; i < queryterm.size(); i++) {
            if (queryterm.get(i).term.equals(term)) {
                return i;
            }
        }

        return -1;
    }
}


