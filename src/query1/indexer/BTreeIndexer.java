package query1.indexer;
import java.io.*; 

import org.apache.lucene.analysis.core.SimpleAnalyzer;    
//import org.apache.lucene.analysis.SimpleAnalyzer;    
import org.apache.lucene.document.Document;    
import org.apache.lucene.document.Field;    
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;    
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory; 
import org.apache.lucene.util.Version;

import java.util.StringTokenizer; 

import org.apache.lucene.analysis.standard.StandardAnalyzer; 

/**
 * 
 * @author klimzaporojets
 * trying to work with b-tree indexer
 */

public class BTreeIndexer {

}
