package query1.indexer;
import java.io.*; 

import org.apache.lucene.analysis.core.SimpleAnalyzer;    
//import org.apache.lucene.analysis.SimpleAnalyzer;    
import org.apache.lucene.document.Document;    
import org.apache.lucene.document.Field;    
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;    
import org.apache.lucene.index.IndexWriterConfig;
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
			int fieldsNumber = fields.length;
			BufferedReader br = new BufferedReader(new FileReader(filePath));
			String line = ""; 
			StringTokenizer st = null;
			int tknNumber=0;
			StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_47);
			IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_47, analyzer);
			
			IndexWriter indexWriter = new IndexWriter(FSDirectory.open(new File(indexDir)),config); 
			
			Document doc = new Document(); 
			while((line=br.readLine())!=null)
			{
				st = new StringTokenizer(line,separator);
				tknNumber=0; 
				
				while(st.hasMoreTokens())
				{
					doc.add(new StringField(fields[tknNumber], st.nextToken(), Field.Store.YES));
					tknNumber++; 
				}
				indexWriter.addDocument(doc);
			}
		}
		catch(Exception ex)
		{
			
		}
	}
}
