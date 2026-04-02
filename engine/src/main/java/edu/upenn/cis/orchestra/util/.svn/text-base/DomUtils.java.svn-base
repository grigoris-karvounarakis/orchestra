package edu.upenn.cis.orchestra.util;

import java.io.OutputStream;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Result;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;

public class DomUtils {
	static public void write(Document doc, Writer out) {
        write(doc,new StreamResult(out));
	}

	static public void write(Document doc, OutputStream out) {
        write(doc,new StreamResult(out));
	}
	
	static public void write(Document doc, Result result) {
		TransformerFactory tfact = TransformerFactory.newInstance();
        try {
        	Transformer trans = tfact.newTransformer();
        	trans.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
        	trans.setOutputProperty(OutputKeys.INDENT, "yes");
        	trans.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
        	trans.setOutputProperty(OutputKeys.METHOD, "xml"); //xml, html, text
        	trans.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
            trans.transform(new DOMSource(doc.getDocumentElement()), result);
        } catch (TransformerConfigurationException e) {
        	assert(false); 	// can't happen
        } catch (TransformerException e) {
        	assert(false);	// can't happen
        }
	}

	static public Element addChild(Document doc, Element parent, String name) { 
		Element child = doc.createElement(name);
		parent.appendChild(child);
		return child;
	}

	static public Element addChildWithText(Document doc, Element parent, String name, String text) { 
		Element child = doc.createElement(name);
		Text content = doc.createTextNode(text);
		parent.appendChild(child);
		child.appendChild(content);
		return child;
	}

	static public Element getChildElementByName(Element parent, String name) {
		NodeList list = parent.getChildNodes();
		for (int i = 0; i < list.getLength(); i++) {
			Node node = list.item(i);
			if (node.getNodeName().equals(name) && node instanceof Element) {
				return (Element)node;
			}
		}
		return null;
	}

	static public List<Element> getChildElementsByName(Element parent, String name) {
		ArrayList<Element> array = new ArrayList<Element>();
		NodeList list = parent.getChildNodes();
		for (int i = 0; i < list.getLength(); i++) {
			Node node = list.item(i);
			if (node.getNodeName().equals(name) && node instanceof Element) {
				array.add((Element)node);
			}
		}
		return array;
	}
	
	static public List<Text> getChildTextNodes(Element parent) {
		ArrayList<Text> retval = new ArrayList<Text>();
		
		Node n = parent.getFirstChild();
		while (n != null) {
			if (n instanceof Text) {
				retval.add((Text) n);
			}
			n = n.getNextSibling();
		}
		
		return retval;
	}
	
	static public List<Element> getChildElements(Element parent) {
		ArrayList<Element> retval = new ArrayList<Element>();
		
		Node n = parent.getFirstChild();
		while (n != null) {
			if (n instanceof Element) {
				retval.add((Element) n);
			}
			n = n.getNextSibling();
		}
		
		return retval;
	}
	
	static public Map<String,String> getAttributes(Element el) {
		Map<String,String> retval = new HashMap<String,String>();
		NamedNodeMap list = el.getAttributes();
		for (int i = 0; i < list.getLength(); i++) {
			Node node = list.item(i);
			if (node instanceof Attr) {
				Attr a = (Attr) node;
				retval.put(a.getName(), a.getValue());
			}
		}
		
		
		return retval;
	}
}
