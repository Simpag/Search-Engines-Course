/**
 *   Computes the Hubs and Authorities for an every document in a query-specific
 *   link graph, induced by the base set of pages.
 *
 *   @author Dmytro Kalpakchi
 */

package ir;

import java.util.*;
import java.io.*;


public class HITSRanker {

    /**
     *   Max number of iterations for HITS
     */
    final static int MAX_NUMBER_OF_STEPS = 1000;

    /**
     *   Convergence criterion: hub and authority scores do not 
     *   change more that EPSILON from one iteration to another.
     */
    final static double EPSILON = 0.001;

    /**
     *   The inverted index
     */
    Index index;

    /**
     *   Mapping from the titles to internal document ids used in the links file
     */
    HashMap<String,Integer> titleToId = new HashMap<String,Integer>();
    HashMap<Integer,String> idToTitle = new HashMap<Integer,String>();

    /* Graph */
    HashMap<Integer,ArrayList<Integer>> outLinks = new HashMap<Integer,ArrayList<Integer>>();
    HashMap<Integer,ArrayList<Integer>> inLinks = new HashMap<Integer,ArrayList<Integer>>();

    /**
     *   The number of outlinks from each node.
     */
    HashMap<Integer, Integer> out = new HashMap<Integer, Integer>();

    /**
     *   Sparse vector containing hub scores
     */
    HashMap<Integer,Double> hubs = new HashMap<Integer, Double>();

    /**
     *   Sparse vector containing authority scores
     */
    HashMap<Integer,Double> authorities = new HashMap<Integer, Double>();

    
    /* --------------------------------------------- */

    /**
     * Constructs the HITSRanker object
     * 
     * A set of linked documents can be presented as a graph.
     * Each page is a node in graph with a distinct nodeID associated with it.
     * There is an edge between two nodes if there is a link between two pages.
     * 
     * Each line in the links file has the following format:
     *  nodeID;outNodeID1,outNodeID2,...,outNodeIDK
     * This means that there are edges between nodeID and outNodeIDi, where i is between 1 and K.
     * 
     * Each line in the titles file has the following format:
     *  nodeID;pageTitle
     *  
     * NOTE: nodeIDs are consistent between these two files, but they are NOT the same
     *       as docIDs used by search engine's Indexer
     *
     * @param      linksFilename   File containing the links of the graph
     * @param      titlesFilename  File containing the mapping between nodeIDs and pages titles
     * @param      index           The inverted index
     */
    public HITSRanker( String linksFilename, String titlesFilename, Index index ) {
        this.index = index;
        readDocs( linksFilename, titlesFilename );
    }


    /* --------------------------------------------- */

    /**
     * A utility function that gets a file name given its path.
     * For example, given the path "davisWiki/hello.f",
     * the function will return "hello.f".
     *
     * @param      path  The file path
     *
     * @return     The file name.
     */
    private String getFileName( String path ) {
        String result = "";
        StringTokenizer tok = new StringTokenizer( path, "\\/" );
        while ( tok.hasMoreTokens() ) {
            result = tok.nextToken();
        }
        return result;
    }


    /**
     * Reads the files describing the graph of the given set of pages.
     *
     * @param      linksFilename   File containing the links of the graph
     * @param      titlesFilename  File containing the mapping between nodeIDs and pages titles
     */
    void readDocs( String linksFilename, String titlesFilename ) {
		try {
			System.err.print( "Reading files... " );
			BufferedReader in = new BufferedReader( new FileReader( linksFilename ));
			String line;
			while ((line = in.readLine()) != null ) {
				int idx = line.indexOf( ";" );
				int idoc = Integer.parseInt(line.substring( 0, idx )); // internal document
				//  Have we seen this document before?
				if ( !outLinks.containsKey(idoc) ) {	
					// This is a previously unseen doc, so add it to the table.
					outLinks.put(idoc, new ArrayList<Integer>());
                    out.put(idoc, 0);
				}
				// Check all outlinks.
				StringTokenizer tok = new StringTokenizer( line.substring(idx+1), "," );
				while ( tok.hasMoreTokens() ) {
					Integer otheriDoc = Integer.parseInt(tok.nextToken());
					// a out link from idoc to otherDoc.
					if ( !outLinks.get(idoc).contains(otheriDoc) ) {
						outLinks.get(idoc).add(otheriDoc);
						out.put(idoc, out.get(idoc) + 1);
					}
                    // Add in links to other doc from idoc
                    if ( inLinks.get(otheriDoc) == null ){
                        inLinks.put(otheriDoc, new ArrayList<Integer>());
                    }
                    if ( !inLinks.get(otheriDoc).contains(idoc) ) {
                        inLinks.get(otheriDoc).add(idoc);
                    }
				}
			}
			in.close();

            BufferedReader in2 = new BufferedReader( new FileReader( titlesFilename ));
			while ((line = in2.readLine()) != null ) {
				int idx = line.indexOf( ";" );
				int idoc = Integer.parseInt(line.substring( 0, idx )); // internal document
                String docFileN = line.substring(idx+1).strip();
				//  Have we seen this document before?
				if ( !titleToId.containsKey(docFileN) ) {	
					// This is a previously unseen doc, so add it to the table.
					titleToId.put(docFileN, idoc);
                    idToTitle.put(idoc, docFileN);
				}
			}
			in2.close();
            System.err.print( "done. " );
		}
		catch ( FileNotFoundException e ) {
			System.err.println( "File " + linksFilename + " not found!" );
		}
		catch ( IOException e ) {
			System.err.println( "Error reading file " + linksFilename );
            e.printStackTrace();
		}
		System.err.println( "Read " + outLinks.size() + " number of documents" );
    }

    /**
     * Perform HITS iterations until convergence
     *
     * @param      titles  The titles of the documents in the root set
     */
    private void iterate(String[] titles) {
        HashMap<Integer,Double> prev_hubs = new HashMap<Integer, Double>();
        HashMap<Integer,Double> prev_authorities = new HashMap<Integer, Double>();
        Integer[] idocs = new Integer[titles.length];
        
        for (int i = 0; i < titles.length; i++) {
            idocs[i] = titleToId.get(titles[i]);
        }

        int iteration = 0;
        init_scores(idocs);
        while (vectorDiffNorm2(hubs, prev_hubs) > EPSILON || vectorDiffNorm2(authorities, prev_authorities) > EPSILON) {
            prev_hubs = copyVector(hubs);
            prev_authorities = copyVector(authorities);

            update_authorities_scores(idocs);
            update_hubs_scores(idocs);
            iteration++;

            System.err.println("Iteration: " + iteration + " ; " + "Hubs: " + vectorDiffNorm2(hubs, prev_hubs) + "; Auth: " + vectorDiffNorm2(authorities, prev_authorities));
        }
    }

    private void iterate(Integer[] rootIDs, Integer[] baseIDs) {
        HashMap<Integer,Double> prev_hubs = new HashMap<Integer, Double>();
        HashMap<Integer,Double> prev_authorities = new HashMap<Integer, Double>();

        init_scores(baseIDs);
        while (vectorDiffNorm2(hubs, prev_hubs) > EPSILON || vectorDiffNorm2(authorities, prev_authorities) > EPSILON) {
            prev_hubs = copyVector(hubs);
            prev_authorities = copyVector(authorities);

            update_authorities_scores(rootIDs);
            update_hubs_scores(rootIDs);
        }
    }

    private void init_scores(Integer[] idocs) {
        for (int idoc : idocs) {
            hubs.put(idoc, 1.0);
            authorities.put(idoc, 1.0);
        }
    }

    private void update_authorities_scores(Integer[] idocs) {
        for (int idoc : idocs) {
            double sum = 0.0;

            if (inLinks.get(idoc) == null)
                continue;

            for (int inLink : inLinks.get(idoc)) {
                sum += hubs.get(inLink);
            }

            authorities.put(idoc, sum);
        }

        normalizeVector(authorities);
    }

    private void update_hubs_scores(Integer[] idocs) {
        for (int idoc : idocs) {
            double sum = 0.0;

            if (outLinks.get(idoc) == null)
                continue;

            for (int outLink : outLinks.get(idoc)){
                sum += authorities.get(outLink);
            }

            hubs.put(idoc, sum);
        }

        normalizeVector(hubs);
    }

    private double vectorDiffNorm2(HashMap<Integer,Double> x, HashMap<Integer,Double> y) {
		double ret = 0;

        Set<Integer> keys = new HashSet<Integer>();
        if (x.size() > 0)
            keys.addAll(x.keySet());
        if (y.size() > 0)
            keys.addAll(y.keySet());

		for (int i : keys) {
            double a = 0.0, b = 0.0;
            if (x.containsKey(i))
                a = x.get(i);
            if (y.containsKey(i))
                b = y.get(i);

			ret += Math.abs(a - b);
		}

		return ret;
	}

    private double vectorNorm2(HashMap<Integer,Double> x) {
        double ret = 0.0;

        for (int i : x.keySet()) {
            ret += Math.pow(x.get(i),2.0);
        }

        return Math.sqrt(ret);
    }

    private void normalizeVector(HashMap<Integer,Double> x) {
        double norm = vectorNorm2(x);

        for (int i : x.keySet()) {
            x.put(i, x.get(i)/norm);
        }
    }

    private HashMap<Integer, Double> copyVector(HashMap<Integer, Double> x) {
        HashMap<Integer, Double> ret = new HashMap<Integer, Double>();

        for (Map.Entry<Integer, Double> entry : x.entrySet()) {
           ret.put(entry.getKey(), entry.getValue());
        }

        return ret;
    }


    /**
     * Rank the documents in the subgraph induced by the documents present
     * in the postings list `post`.
     *
     * @param      post  The list of postings fulfilling a certain information need
     * @param      ratio The mixing between hub score (ratio) and authority score (1-ratio).
     *
     * @return     A list of postings ranked according to the hub and authority scores.
     */
    PostingsList rank(PostingsList post, double ratio) {
        Set<Integer> docIDs = new HashSet<Integer>(); 
        Set<Integer> rootSet = new HashSet<Integer>();
        Set<Integer> baseSet = new HashSet<Integer>(); // Set of all documents that link to or linked from root set

        Iterator<PostingsEntry> iter = post.iterator();
        while (iter.hasNext()) {
            PostingsEntry e = iter.next();

            String title = getFileName(Index.docNames.get(e.docID));
            int id = titleToId.get(title);
            docIDs.add(e.docID);
            baseSet.add(id);
            rootSet.add(id);

            // Add all in and out documents
            if (outLinks.containsKey(id)) {
                for (int out : outLinks.get(id)) {
                    baseSet.add(out);
                }
            }
            
            if (inLinks.containsKey(id)) {
                for (int in : inLinks.get(id)) {
                    baseSet.add(in);
                }
            }
        }

        iterate(rootSet.toArray(new Integer[rootSet.size()]), baseSet.toArray(new Integer[baseSet.size()]));
        
        // Set scoring for each doc
        for (int docID : docIDs) {
            String title = getFileName(Index.docNames.get(docID));
            int id = titleToId.get(title);

            double hub_score = hubs.get(id);
            double auth_score = authorities.get(id);
            double score = ratio*hub_score + auth_score*ratio;

            post.addScore(docID, score);
        }
        
        return post;
    }


    /**
     * Sort a hash map by values in the descending order
     *
     * @param      map    A hash map to sorted
     *
     * @return     A hash map sorted by values
     */
    private HashMap<Integer,Double> sortHashMapByValue(HashMap<Integer,Double> map) {
        if (map == null) {
            return null;
        } else {
            List<Map.Entry<Integer,Double> > list = new ArrayList<Map.Entry<Integer,Double> >(map.entrySet());
      
            Collections.sort(list, new Comparator<Map.Entry<Integer,Double>>() {
                public int compare(Map.Entry<Integer,Double> o1, Map.Entry<Integer,Double> o2) { 
                    return (o2.getValue()).compareTo(o1.getValue()); 
                } 
            }); 
              
            HashMap<Integer,Double> res = new LinkedHashMap<Integer,Double>(); 
            for (Map.Entry<Integer,Double> el : list) { 
                res.put(el.getKey(), el.getValue()); 
            }
            return res;
        }
    } 


    /**
     * Write the first `k` entries of a hash map `map` to the file `fname`.
     *
     * @param      map        A hash map
     * @param      fname      The filename
     * @param      k          A number of entries to write
     */
    void writeToFile(HashMap<Integer,Double> map, String fname, int k) {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(fname));
            
            if (map != null) {
                int i = 0;
                for (Map.Entry<Integer,Double> e : map.entrySet()) {
                    i++;
                    writer.write(e.getKey() + ": " + String.format("%.5g%n", e.getValue()));
                    if (i >= k) break;
                }
            }
            writer.close();
        } catch (IOException e) {}
    }


    /**
     * Rank all the documents in the links file. Produces two files:
     *  hubs_top_30.txt with documents containing top 30 hub scores
     *  authorities_top_30.txt with documents containing top 30 authority scores
     */
    void rank() {
        iterate(titleToId.keySet().toArray(new String[0]));
        HashMap<Integer,Double> sortedHubs = sortHashMapByValue(hubs);
        HashMap<Integer,Double> sortedAuthorities = sortHashMapByValue(authorities);
        writeToFile(sortedHubs, "hubs_top_30.txt", 30);
        writeToFile(sortedAuthorities, "authorities_top_30.txt", 30);
    }


    /* --------------------------------------------- */


    public static void main( String[] args ) {
        if ( args.length != 2 ) {
            System.err.println( "Please give the names of the link and title files" );
        }
        else {
            HITSRanker hr = new HITSRanker( args[0], args[1], null );
            hr.rank();
        }
    }
} 