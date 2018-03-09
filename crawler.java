import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.Scanner;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class crawler {
	public void crawl(String URL){
		try {
			Document doc = Jsoup.connect(URL).get();
			Elements link = doc.select("a[href]");
			PrintWriter out;
			String filename = "";
			
			for( Element page : link){
				String absHref = (page.attr("abs:href"));// == "/"
				int index = absHref.lastIndexOf('/');
				
				if(index == absHref.length() - 1){
						continue;
				}
				
				filename = absHref.substring(index + 1);
				filename = "/Users/nancyli/Desktop/179_data/".concat(filename);
				System.out.println(filename);
				out = new PrintWriter(filename);
				
				URL url = new URL(absHref);
				URLConnection conn = url.openConnection();
				BufferedReader in = new BufferedReader(
						new InputStreamReader(conn.getInputStream()));
				String inputLine = "";
				while ((inputLine = in.readLine()) != null){
					out.println(inputLine);
				}
				in.close();
				out.close();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
