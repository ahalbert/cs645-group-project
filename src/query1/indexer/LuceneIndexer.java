package query1.indexer;
import java.io.*; 

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;    
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
//import org.apache.lucene.analysis.SimpleAnalyzer;    
import org.apache.lucene.document.Document;    
import org.apache.lucene.document.Field;    
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;    
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory; 
import org.apache.lucene.util.Version;

import java.util.StringTokenizer; 

import org.apache.lucene.analysis.standard.StandardAnalyzer; 

/**
 * 
 * @author klimzaporojets
 * trying to work with Lucene indexer
 */

public class LuceneIndexer {
	public static String indexDir = "/Users/klimzaporojets/klim/umass/CMPSCI645 Database Design and Implementation/project topics/social_networks/index_files";
	public static String indexDir2 = "/Users/klimzaporojets/klim/umass/CMPSCI645 Database Design and Implementation/project topics/social_networks/comment_creator";
	public static String indexDirV2 = "/Users/klimzaporojets/klim/umass/CMPSCI645 Database Design and Implementation/project topics/social_networks/index_files_v2";
	
	public void indexFile(String filePath, String indexDir, String separator, String [] fields) 
	{
		try
		{
			int counter = 0;
			int fieldsNumber = fields.length;
			BufferedReader br = new BufferedReader(new FileReader(filePath));
			String line = ""; 
			StringTokenizer st = null;
			int tknNumber=0;
//			StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_47);
//			SimpleAnalyzer analyzer = new SimpleAnalyzer(Version.LUCENE_47);
			Analyzer analyzer = new WhitespaceAnalyzer(Version.LUCENE_47);
			IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_47, analyzer);
			config.setRAMBufferSizeMB(500);
			config.setUseCompoundFile(false);
			
			IndexWriter indexWriter = new IndexWriter(FSDirectory.open(new File(indexDir)),config); 

			while((line=br.readLine())!=null)
			{
				Document doc = new Document(); 
				st = new StringTokenizer(line,separator);
				tknNumber=0; 
				if(++counter%10000==0)
				{
					System.out.println(counter);
				}
				while(st.hasMoreTokens())
				{
					doc.add(new StringField(fields[tknNumber], st.nextToken(), Field.Store.YES));
					tknNumber++; 
				}
				indexWriter.addDocument(doc);
			}
//			indexWriter.
			indexWriter.close();
			
		}
		catch(Exception ex)
		{
			System.out.println("Error has happened");
			ex.printStackTrace();
		}
	}

	//TODO: the idea is to index this one combining several values in the same document, in this way grouping it by the desired elements
	public void indexFileV3(String filePath, String separator, String [] fields) 
	{
		try
		{
			int counter = 0;
			int fieldsNumber = fields.length;
			BufferedReader br = new BufferedReader(new FileReader(filePath));
			String line = ""; 
			StringTokenizer st = null;
			int tknNumber=0;
			StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_47);
			IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_47, analyzer);
			
			IndexWriter indexWriter = new IndexWriter(FSDirectory.open(new File(indexDir)),config); 
			
			
			while((line=br.readLine())!=null)
			{
				Document doc = new Document(); 
				st = new StringTokenizer(line,separator);
				tknNumber=0; 
				if(++counter%1000==0)
				{
					System.out.println(counter);
				}
				while(st.hasMoreTokens())
				{
					doc.add(new StringField(fields[tknNumber], st.nextToken(), Field.Store.YES));
					tknNumber++; 
				}
				indexWriter.addDocument(doc);
			}
//			indexWriter.
			indexWriter.close();
			
		}
		catch(Exception ex)
		{
			System.out.println("Error has happened");
			ex.printStackTrace();
		}
	}	
	
	public void indexQuery1(String commentsIndexPath, String commentsReplyFilePath){
		try{
			Directory directory = FSDirectory.open(new File(commentsIndexPath));
			SearcherManager manager = new SearcherManager(directory, new SearcherFactory());
			
			IndexSearcher s = manager.acquire(); 
			try
			{
				//builds the index with person_responder,comment_responded,comment_published,person_publisher
				
			}
			finally
			{
				manager.release(s);
			}
		}
		catch(Exception ex)
		{
			
		}
	}
	
	public void searchIndex(File indexDir, String queryStr, int maxHits) {
		try {
			Directory directory = FSDirectory.open(indexDir);
			IndexSearcher searcher = new IndexSearcher(
					DirectoryReader.open(directory));
			QueryParser parser = new QueryParser(Version.LUCENE_47, "contents",
					new StandardAnalyzer(Version.LUCENE_47)/*
															 * new
															 * SimpleAnalyzer()
															 */);
			Query query = parser.parse(queryStr);
			TopDocs topDocs = searcher.search(query, maxHits);
			ScoreDoc[] hits = topDocs.scoreDocs;

			for (int i = 0; i < hits.length; i++) {

				int docId = hits[i].doc;
				Document d = searcher.doc(docId);
				System.out.println(d.get("comment_to"));

			}
			System.out.println("Found " + hits.length);

		} catch (Exception ex) {
			System.out.println("Exception has happened");
			ex.printStackTrace();
		}
	}
	
	public void searchIndexV2(File indexDir, String queryStr, int maxHits) {
		try {
			Directory directory = FSDirectory.open(indexDir);
			IndexSearcher searcher = new IndexSearcher(
					DirectoryReader.open(directory));
			QueryParser parser = new QueryParser(Version.LUCENE_47, "contents",
					new SimpleAnalyzer(Version.LUCENE_47)/*
															 * new
															 * SimpleAnalyzer()
															 */);
			Query query = parser.parse(queryStr);
			TopDocs topDocs = searcher.search(query, maxHits);
			ScoreDoc[] hits = topDocs.scoreDocs;

			for (int i = 0; i < hits.length; i++) {

				int docId = hits[i].doc;
				Document d = searcher.doc(docId);
				System.out.println(d.get("comment_to"));

			}
			System.out.println("Found " + hits.length);

		} catch (Exception ex) {
			System.out.println("Exception has happened");
			ex.printStackTrace();
		}
	}	
	//trying to index the whole file at once, without going row by row
	public void indexFileV2(String filePath, String separator, String [] fields) 
	{
		try
		{
			SimpleAnalyzer analyzer = new SimpleAnalyzer(Version.LUCENE_47);
			IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_47, analyzer);
			
			IndexWriter indexWriter = new IndexWriter(FSDirectory.open(new File(indexDirV2)),config);
			Document document = new Document();
			File file = new File(filePath);
//			String path = file.getCanonicalPath();
			document.add(new TextField("contents", new FileReader(file)));


			document.add(new StringField("file_name", filePath, Field.Store.YES));

			indexWriter.addDocument(document);
		}
		catch(Exception ex)
		{
			System.out.println("Error has happened");
			ex.printStackTrace();
		}
	}	
	public static boolean isIndexCreated(String indexPath)
	{
		try{
			Directory directory = FSDirectory.open(new File(indexPath));
			IndexSearcher searcher = new IndexSearcher(
				DirectoryReader.open(directory));
		}
		catch(Exception ex)
		{
			return false; 
		}
		return true; 
	}
	public static void main (String [] args)
	{
		LuceneIndexer luceneIndexer = new LuceneIndexer(); 
//		luceneIndexer.indexFileV2("/Users/klimzaporojets/klim/umass/CMPSCI645 Database Design and Implementation/project"
//				+ " topics/social_networks/big_data_files/comment_replyOf_comment.csv",
//					"|", new String[]{"comment_from","comment_to"});
		
//		luceneIndexer.indexFile("/Users/klimzaporojets/klim/umass/CMPSCI645 Database Design and Implementation/project"
//				+ " topics/social_networks/big_data_files/comment_replyOf_comment.csv",indexDir,
//					"|", new String[]{"comment_from","comment_to"});
		luceneIndexer.indexFile("/Users/klimzaporojets/klim/umass/CMPSCI645 Database Design and Implementation/project"
				+ " topics/social_networks/big_data_files/comment_hasCreator_person.csv", indexDir2,
					"|", new String[]{"comment","person"});
		
		
		luceneIndexer.searchIndex(new File(indexDir),  "comment_from:1380", 999999);
//		luceneIndexer.searchIndexV2(new File(indexDirV2), "contents:1380|", 999999);

		System.out.println("File indexed");
	}
}
