package query1.loaders;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;
import org.xml.sax.SAXException;

import com.foundations.comparator.structure.IDataStructureReader;
import com.foundations.comparator.structure.RowComparator;
import com.foundations.comparator.structure.XMLStructureReader;
import com.google.code.externalsorting.ExternalSort;

/**
 * 
 * @author klimzaporojets
 * Manages/sorts the csv files  
 */
public class FileManagerQuery1 {
	
	private static final int DEFAULTMAXTEMPFILES = 10240;
//	private static final File XML_FILE = new File("layout.xml");
//	private static final File INPUT_FILE = new File("Canada.csv");
//	private static final File OUTPUT_FILE = new File("Canada.sort.csv");
//	private static final File SORT_FOLDER = new File("TEMP");

	public void sort(String filePath, String fileName, String xmlPath, String xmlFile, String sortedPath, String sortedFile) throws IOException, ParserConfigurationException, SAXException {
		File XML_FILE = new File(xmlPath + xmlFile);
		IDataStructureReader reader = new XMLStructureReader(XML_FILE);
		RowComparator comparator = new RowComparator(reader);
		
		File SORT_FOLDER = new File(sortedPath);
		File INPUT_FILE = new File(filePath + fileName); 
		
		File OUTPUT_FILE = new File(sortedPath + sortedFile);

		List<File> fileList = ExternalSort.sortInBatch(INPUT_FILE, comparator, DEFAULTMAXTEMPFILES, Charset.defaultCharset(), SORT_FOLDER, false);
		
		ExternalSort.mergeSortedFiles(fileList, OUTPUT_FILE, comparator, Charset.defaultCharset(), false);
	}

	public static void main(String[] args) throws IOException, ParserConfigurationException, SAXException {
		FileManagerQuery1 app = new FileManagerQuery1();
		Long start = System.currentTimeMillis();
		String filePath="/Users/klimzaporojets/klim/umass/CMPSCI645 Database Design and Implementation/project topics/social_networks/big_data_files/"; 
		String fileName="comment_hasCreator_person_no_header.csv"; 
		String xmlPath="/Users/klimzaporojets/klim/umass/CMPSCI645 Database Design and Implementation/project topics/social_networks/xml_conf/"; 
		String xmlFile="layout_comment_creator1.xml"; 
		String sortedPath="/Users/klimzaporojets/klim/umass/CMPSCI645 Database Design and Implementation/project topics/social_networks/sorted_files/"; 
		String sortedFile="comment_hasCreator_person_sf2.csv"; 
		
		app.sort(filePath, fileName, xmlPath, xmlFile, sortedPath, sortedFile);
		Long stop = System.currentTimeMillis();
		System.out.println(((stop - start)/1000) + " seconds");
	}

}
