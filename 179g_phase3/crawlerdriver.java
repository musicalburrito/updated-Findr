import java.util.Scanner;

public class crawlerdriver {
	public static void main(String[] args){
		crawler temp = new crawler();
		System.out.println("Enter a URL: ");
		Scanner input = new Scanner(System.in);
		String URL = input.next();
		input.close();
		
		temp.crawl(URL);
	}
}
