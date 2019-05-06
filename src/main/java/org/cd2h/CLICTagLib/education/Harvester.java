package org.cd2h.CLICTagLib.education;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import edu.uiowa.util.PropertyLoader;

public class Harvester {
    static Logger logger = Logger.getLogger(Harvester.class);
    static Properties prop_file = PropertyLoader.loadProperties("cd2h");
    static Connection conn = null;
    static int id = 0;

    public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
	PropertyConfigurator.configure(args[0]);
	conn = getConnection();
	harvestPage(0);
	harvestPage(1);
//	harvestResource("https://clic-ctsa.org/education/diamond");
    }
    
    static void harvestPage(int pageNum) throws IOException, SQLException {
	Document doc = Jsoup.connect("https://clic-ctsa.org/education?page=" + pageNum).timeout(0).get();
	logger.trace("doc: " + doc.toString());
	for (Element element : doc.getElementsByTag("h5")) {
	    String url = element.getElementsByTag("a").first().attr("href");
	    harvestResource(++id, "https://clic-ctsa.org"+url);
	}
	
    }
    
    static void harvestResource(int id, String url) throws IOException, SQLException {
	logger.info("url: " + url);
	Document doc = Jsoup.connect(url).timeout(0).get();
	logger.debug("doc: " + doc.toString());
	String title = null;
	String description = null;
	String objective = null;
	String institution = null;
	String method = null;
	String frequency = null;
	String length = null;
	String fee = null;
	
	for (Element element : doc.getElementsByClass("page-header")) {
	    title = element.text();
	    logger.info("title: " + title);
	}
	for (Element element : doc.getElementsByClass("field-name-body")) {
	    description = element.text();
	    logger.info("description: " + description);
	}
	for (Element element : doc.getElementsByClass("field-name-field-learning-objectives")) {
	    for (Element subelement : element.getElementsByClass("field-item")) {
		objective = subelement.text();
		logger.info("objective: " + objective);
	    }
	}
	for (Element element : doc.getElementsByClass("views-field-field-institution")) {
	    for (Element subelement : element.getElementsByClass("field-content")) {
		institution = subelement.text();
		logger.info("institution: " + institution);
	    }
	}
	for (Element element : doc.getElementsByClass("views-field-field-delivery-method")) {
	    for (Element subelement : element.getElementsByClass("field-content")) {
		method = subelement.text();
		logger.info("method: " + method);
	    }
	}
	for (Element element : doc.getElementsByClass("views-field-field-frequency")) {
	    for (Element subelement : element.getElementsByClass("field-content")) {
		frequency = subelement.text();
		logger.info("frequency: " + frequency);
	    }
	}
	for (Element element : doc.getElementsByClass("views-field-field-length-of-course")) {
	    for (Element subelement : element.getElementsByClass("field-content")) {
		length = subelement.text();
		logger.info("length: " + length);
	    }
	}
	for (Element element : doc.getElementsByClass("views-field-field-fee")) {
	    for (Element subelement : element.getElementsByClass("field-content")) {
		fee = subelement.text();
		logger.info("fee: " + fee);
	    }
	}
	
	PreparedStatement item = conn.prepareStatement("insert into clic.resource values(?,?,?,?,?,?,?,?,?)");
	item.setInt(1, id);
	item.setString(2, url);
	item.setString(3, title);
	item.setString(4, description);
	item.setString(5, objective);
	item.setString(6, institution);
	item.setString(7, method);
	item.setString(8, frequency);
	item.setString(9, fee);
	item.execute();
	item.close();
	
	int seqnum = 0;
	for (Element element : doc.getElementsByClass("field-name-field-competencies")) {
	    for (Element subelement : element.getElementsByClass("field-item")) {
		String competency = subelement.text();
		logger.info("competency: " + competency);
		PreparedStatement sub = conn.prepareStatement("insert into clic.competency values(?,?,?)");
		sub.setInt(1, id);
		sub.setInt(2, ++seqnum);
		sub.setString(3, competency);
		sub.execute();
		sub.close();
	    }
	}
	seqnum = 0;
	for (Element element : doc.getElementsByClass("field-name-field-target-learners")) {
	    for (Element subelement : element.getElementsByClass("field-item")) {
		String target = subelement.text();
		logger.info("target: " + target);
		PreparedStatement sub = conn.prepareStatement("insert into clic.target values(?,?,?)");
		sub.setInt(1, id);
		sub.setInt(2, ++seqnum);
		sub.setString(3, target);
		sub.execute();
		sub.close();
	    }
	}
	seqnum = 0;
	for (Element element : doc.getElementsByClass("tag-wrapper")) {
	    for (Element subelement : element.getElementsByTag("a")) {
		String tag = subelement.text();
		logger.info("tag: " + tag);
		PreparedStatement sub = conn.prepareStatement("insert into clic.tag values(?,?,?)");
		sub.setInt(1, id);
		sub.setInt(2, ++seqnum);
		sub.setString(3, tag);
		sub.execute();
		sub.close();
	    }
	}
    }

    static Connection getConnection() throws ClassNotFoundException, SQLException {
	Class.forName("org.postgresql.Driver");
	Properties props = new Properties();
	props.setProperty("user", prop_file.getProperty("jdbc.user"));
	props.setProperty("password", prop_file.getProperty("jdbc.password"));
//	if (use_ssl.equals("true")) {
//	    props.setProperty("sslfactory", "org.postgresql.ssl.NonValidatingFactory");
//	    props.setProperty("ssl", "true");
//	}
	Connection conn = DriverManager.getConnection(prop_file.getProperty("jdbc.url"), props);
//	 conn.setAutoCommit(false);

	return conn;

    }

    static void executeCommand(String command) throws SQLException {
	logger.info(command + "...");
	conn.prepareStatement(command).execute();
    }

}
