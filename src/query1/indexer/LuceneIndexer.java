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
 * trying to work with Lucene indexer
 */

public class LuceneIndexer {
	public static String indexDir = "/Users/klimzaporojets/klim/umass/CMPSCI645 Database Design and Implementation/project topics/social_networks/index_files";
	
	public void indexFile(String filePath, String separator, String [] fields) 
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
	
	public void indexFileV2(String filePath, String separator, String [] fields) 
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
	
	public static void main (String [] args)
	{
		LuceneIndexer luceneIndexer = new LuceneIndexer(); 
		luceneIndexer.indexFile("/Users/klimzaporojets/klim/umass/CMPSCI645 Database Design and Implementation/project"
				+ " topics/social_networks/big_data_files/comment_replyOf_comment.csv",
					"|", new String[]{"comment_from","comment_to"});
		luceneIndexer.searchIndex(new File(indexDir), "comment_from:1380", 999999);

		System.out.println("File indexed");
	}
}
