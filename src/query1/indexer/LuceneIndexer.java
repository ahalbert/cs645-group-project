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
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer; 

import org.apache.lucene.analysis.standard.StandardAnalyzer; 

/**
 * 
 * @author klimzaporojets
 * trying to work with Lucene indexer
 */

public class LuceneIndexer {
//	public static String indexDir = "/Users/klimzaporojets/klim/umass/CMPSCI645 Database Design and Implementation/project topics/social_networks/index_files";
//	public static String indexDir2 = "/Users/klimzaporojets/klim/umass/CMPSCI645 Database Design and Implementation/project topics/social_networks/comment_creator";
//	public static String indexDirV2 = "/Users/klimzaporojets/klim/umass/CMPSCI645 Database Design and Implementation/project topics/social_networks/index_files_v2";
	
	
	static Directory directoryPerson = null;
	
	static SearcherManager managerPerson = null;
	
	static IndexSearcher sPerson;
	
	static Directory directoryComments = null;
	
	static SearcherManager managerComments = null;
	
	static IndexSearcher sComments = null;
	

	public void indexFile(String filePath, String indexDir, String separator, String [] fields, boolean override) 
	{
		try
		{
			if(isIndexCreated(indexDir)&&!override)
			{
				return; 
			}
			int counter = 0;
			int fieldsNumber = fields.length;
			BufferedReader br = new BufferedReader(new FileReader(filePath));
			String line = ""; 
			StringTokenizer st = null;
			int tknNumber=0;
//			StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_47);
			SimpleAnalyzer analyzer = new SimpleAnalyzer(Version.LUCENE_47);
//			Analyzer analyzer = new WhitespaceAnalyzer(Version.LUCENE_47);
			IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_47, analyzer);
			config.setRAMBufferSizeMB(1000);
			config.setUseCompoundFile(false);
			
			IndexWriter indexWriter = new IndexWriter(FSDirectory.open(new File(indexDir)),config); 

			while((line=br.readLine())!=null)
			{
				Document doc = new Document(); 
				st = new StringTokenizer(line,separator);
				tknNumber=0; 
//				System.out.println(line);
				if(++counter%100000==0)
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
			indexWriter.close();
			
		}
		catch(Exception ex)
		{
			System.out.println("Error has happened");
			ex.printStackTrace();
		}
	}

	//TODO: the idea is to index this one combining several values in the same document, in this way grouping it by the desired elements
//	public void indexFileV3(String filePath, String separator, String [] fields) 
//	{
//		try
//		{
//			int counter = 0;
//			int fieldsNumber = fields.length;
//			BufferedReader br = new BufferedReader(new FileReader(filePath));
//			String line = ""; 
//			StringTokenizer st = null;
//			int tknNumber=0;
//			StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_47);
//			IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_47, analyzer);
//			
//			IndexWriter indexWriter = new IndexWriter(FSDirectory.open(new File(indexDir)),config); 
//			
//			
//			while((line=br.readLine())!=null)
//			{
//				Document doc = new Document(); 
//				st = new StringTokenizer(line,separator);
//				tknNumber=0; 
//				if(++counter%1000==0)
//				{
//					System.out.println(counter);
//				}
//				while(st.hasMoreTokens())
//				{
//					doc.add(new StringField(fields[tknNumber], st.nextToken(), Field.Store.YES));
//					tknNumber++; 
//				}
//				indexWriter.addDocument(doc);
//			}
////			indexWriter.
//			indexWriter.close();
//			
//		}
//		catch(Exception ex)
//		{
//			System.out.println("Error has happened");
//			ex.printStackTrace();
//		}
//	}	
	//this one works only with lucene
	public void indexQuery1(String replyIndexPath,String commentsIndexPath, String commentsReplyFilePath){
		try{
			Directory directory = FSDirectory.open(new File(replyIndexPath));
			SearcherManager manager = new SearcherManager(directory, new SearcherFactory());
			Analyzer analyzer = new WhitespaceAnalyzer(Version.LUCENE_47);
			IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_47, analyzer);
			config.setRAMBufferSizeMB(500);
			config.setUseCompoundFile(false);
			
			IndexWriter indexWriter = new IndexWriter(FSDirectory.open(new File(commentsIndexPath)),config); 
			
			IndexSearcher s = manager.acquire(); 
			int counter=0; 
			try
			{
				//builds the index with person_responder,comment_responded,comment_published,person_publisher
				BufferedReader br = new BufferedReader(new FileReader(commentsReplyFilePath));
				String line = ""; 
				QueryParser parser = new QueryParser(Version.LUCENE_47, "comment",
						new WhitespaceAnalyzer(Version.LUCENE_47));			
				line = br.readLine(); 
				while((line=br.readLine())!=null)
				{
					if(++counter%1000==0)
					{
						System.out.println(counter);
					}
					StringTokenizer st = new StringTokenizer(line,"|");
					String comment1 = st.nextToken(); 
					String comment2 = st.nextToken();
					
					Query query = parser.parse("comment:" + comment1);
					
					TopDocs topDocs = s.search(query, 1);

					ScoreDoc[] hits = topDocs.scoreDocs;
					String person1=""; 
					for (int i = 0; i < hits.length; i++) {

						int docId = hits[i].doc;
						Document d = s.doc(docId);
						person1 = d.get("person");

					}
					
					
					query = parser.parse("comment:" + comment2);
					topDocs = s.search(query, 1);

					hits = topDocs.scoreDocs;
					String person2=""; 
					for (int i = 0; i < hits.length; i++) {

						int docId = hits[i].doc;
						Document d = s.doc(docId);
						person2 = d.get("person");

					}
					Document doc = new Document(); 
					doc.add(new StringField("person_responder", person1, Field.Store.YES));
					doc.add(new StringField("comment_responded", comment1, Field.Store.YES));
					doc.add(new StringField("comment_published", comment2, Field.Store.YES));
					doc.add(new StringField("person_publisher", person2, Field.Store.YES));
					indexWriter.addDocument(doc);
					
				}	
				indexWriter.close();
			}
			finally
			{
				manager.release(s);
			}
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
	}
	
	//this one works with data preloaded in the memory (in commentToPerson parameter)
	public void indexQuery1V2(String replyIndexPath,String commentsIndexPath, String commentsReplyFilePath, HashMap<Integer, Integer> commentToPerson){
		try{
			if(isIndexCreated(commentsIndexPath))
			{
				return; 
			}
			
			Directory directory = FSDirectory.open(new File(replyIndexPath));
//			SearcherManager manager = new SearcherManager(directory, new SearcherFactory());
			Analyzer analyzer = new WhitespaceAnalyzer(Version.LUCENE_47);
			IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_47, analyzer);
			config.setRAMBufferSizeMB(50);
			config.setUseCompoundFile(false);
			
			IndexWriter indexWriter = new IndexWriter(FSDirectory.open(new File(commentsIndexPath)),config); 
			
//			IndexSearcher s = manager.acquire(); 
			int counter=0; 
			try
			{
				//builds the index with person_responder,comment_responded,comment_published,person_publisher
				BufferedReader br = new BufferedReader(new FileReader(commentsReplyFilePath));
				String line = ""; 
				QueryParser parser = new QueryParser(Version.LUCENE_47, "comment",
						new WhitespaceAnalyzer(Version.LUCENE_47));			
				line = br.readLine(); 
				while((line=br.readLine())!=null)
				{
					if(++counter%1000==0)
					{
						System.out.println(counter);
					}
					StringTokenizer st = new StringTokenizer(line,"|");
					String comment1 = st.nextToken(); 
					String comment2 = st.nextToken();
					
//					Query query = parser.parse("comment:" + comment1);
					
//					TopDocs topDocs = s.search(query, 1);
//
//					ScoreDoc[] hits = topDocs.scoreDocs;
//					String person1=""; 
//					for (int i = 0; i < hits.length; i++) {
//
//						int docId = hits[i].doc;
//						Document d = s.doc(docId);
//						person1 = d.get("person");
//
//					}
					
					
//					query = parser.parse("comment:" + comment2);
//					topDocs = s.search(query, 1);
//
//					hits = topDocs.scoreDocs;
//					String person2=""; 
//					for (int i = 0; i < hits.length; i++) {
//
//						int docId = hits[i].doc;
//						Document d = s.doc(docId);
//						person2 = d.get("person");
//
//					}
					Integer person1 = commentToPerson.get(Integer.valueOf(comment1));
					Integer person2 = commentToPerson.get(Integer.valueOf(comment2));
					
					Document doc = new Document(); 
					doc.add(new StringField("person_responder", person1.toString(), Field.Store.YES));
					doc.add(new StringField("comment_responded", comment1, Field.Store.YES));
					doc.add(new StringField("comment_published", comment2, Field.Store.YES));
					doc.add(new StringField("person_publisher", person2.toString(), Field.Store.YES));
					indexWriter.addDocument(doc);
					
				}	
				indexWriter.close();
			}
			finally
			{
//				manager.release(s);
			}
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
	}	
	
	public ArrayList<Integer> getUsersConnected(Integer userId, String indexPath)
	{
		ArrayList<Integer> retValue = new ArrayList<Integer>(); 
		try
		{
			if (sPerson==null)
			{
				directoryPerson = FSDirectory.open(new File(indexPath));
				
				managerPerson = new SearcherManager(directoryPerson, new SearcherFactory());
				
				sPerson = managerPerson.acquire();
			}
			try
			{
				Analyzer analyzer = new WhitespaceAnalyzer(Version.LUCENE_47); 
				QueryParser parser = new QueryParser(Version.LUCENE_47,"person_from", analyzer);
				String query = "person_from:" + userId;
				
				Query qQuery = parser.parse(query);

				
				TopDocs topDocs = sPerson.search(qQuery,9999999);

				ScoreDoc[] hits = topDocs.scoreDocs;
				
				for(ScoreDoc sd:hits)
				{
//					retValue.add(sd.doc); 
					Document d = sPerson.doc(sd.doc);
					retValue.add(Integer.valueOf(d.get("person_to")));					
				}
			}
			catch(Exception ex)
			{
				ex.printStackTrace();
			}
			finally
			{
				managerPerson.close();
			}
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
		return retValue; 
	}
	public boolean isEnoughComments(Integer userFrom, Integer userTo, Integer Comments, String indexPath)
	{
		try
		{
			if(sComments==null)
			{
				directoryComments = FSDirectory.open(new File(indexPath));
			
				managerComments = new SearcherManager(directoryComments, new SearcherFactory());
			
				sComments = managerComments.acquire();
			}
			try
			{
				Analyzer analyzer = new WhitespaceAnalyzer(Version.LUCENE_47); 
				QueryParser parser = new MultiFieldQueryParser(Version.LUCENE_47, new String[] {"person_responder", "comment_responded", "comment_published", "person_publisher"}, analyzer);
				String query = "person_responder:" + userFrom + " AND person_publisher:" + userTo;
				
				Query qQuery = parser.parse(query);

				
				TopDocs topDocs = sComments.search(qQuery, Comments+1);

				ScoreDoc[] hits = topDocs.scoreDocs;
				if(hits.length>Comments)
				{
					query = "person_responder:" + userTo + " AND person_publisher:" + userFrom;
					
					qQuery = parser.parse(query);

					
					topDocs = sComments.search(qQuery, Comments+1);
					
					hits = topDocs.scoreDocs; 
					if(hits.length>Comments)
					{
						return true; 						
					}
				}
			}
			catch(Exception ex)
			{
				ex.printStackTrace();
			}
			finally
			{
				managerComments.close();
			}
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
		return false; 
	}
	
	
	public void searchIndex(File indexDir, String queryStr, int maxHits) {
		try {
			Directory directory = FSDirectory.open(indexDir);
			IndexSearcher searcher = new IndexSearcher(
					DirectoryReader.open(directory));
			QueryParser parser = new QueryParser(Version.LUCENE_47, "comment",
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
//	public void indexFileV2(String filePath, String separator, String [] fields) 
//	{
//		try
//		{
//			SimpleAnalyzer analyzer = new SimpleAnalyzer(Version.LUCENE_47);
//			IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_47, analyzer);
//			
//			IndexWriter indexWriter = new IndexWriter(FSDirectory.open(new File(indexDirV2)),config);
//			Document document = new Document();
//			File file = new File(filePath);
////			String path = file.getCanonicalPath();
//			document.add(new TextField("contents", new FileReader(file)));
//
//
//			document.add(new StringField("file_name", filePath, Field.Store.YES));
//
//			indexWriter.addDocument(document);
//		}
//		catch(Exception ex)
//		{
//			System.out.println("Error has happened");
//			ex.printStackTrace();
//		}
//	}	
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
//	public static void main (String [] args)
//	{
//		LuceneIndexer luceneIndexer = new LuceneIndexer(); 
//		luceneIndexer.indexFile("/Users/klimzaporojets/klim/umass/CMPSCI645 Database Design and Implementation/project"
//				+ " topics/social_networks/big_data_files/comment_hasCreator_person.csv", indexDir2,
//					"|", new String[]{"comment","person"},true);
//		
//		
//		luceneIndexer.searchIndex(new File(indexDir),  "comment_from:1380", 999999);
////		luceneIndexer.searchIndexV2(new File(indexDirV2), "contents:1380|", 999999);
//
//		System.out.println("File indexed");
//	}
}
