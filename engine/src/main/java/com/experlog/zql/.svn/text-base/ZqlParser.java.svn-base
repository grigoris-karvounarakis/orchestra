package com.experlog.zql;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.Reader;
import java.util.Vector;

/**
 * ZqlParser: an SQL parser
 */
public class ZqlParser {

  ZqlJJParser _parser = null;

  /**
   * Test program:
   * Parses SQL statements from stdin or from a text file.<br>
   * If the program receives one argument, it is an SQL text file name;
   * if there's no argument, the program reads from stdin.
   */
  public static void main(String args[]) throws ParseException {

    ZqlParser p = null;

    if ( args.length < 1  ) {
      System.out.println("/* Reading from stdin (exit; to finish) */");
      p = new ZqlParser(System.in);

    } else {

      try {
        p = new ZqlParser(new DataInputStream(new FileInputStream(args[0])));
      } catch (FileNotFoundException e) {
        System.out.println("/* File " + args[0] +
         " not found. Reading from stdin */");
        p = new ZqlParser(System.in);
      }
    } // else ends here

    if ( args.length > 0 ) {
      System.out.println("/* Reading from " + args[0] + "*/");
    }

    ZStatement st = null;
    while((st = p.readStatement()) != null) {
      System.out.println(st.toString() + ";");
    }

    System.out.println("exit;");
    System.out.println("/* Parse Successful */") ;

  } // main ends here


  /**
   * Create a new parser to parse SQL statements from a given input stream.
   * @param in The InputStream from which SQL statements will be read.
   */
  public ZqlParser(InputStream in) {
    initParser(in);
  }
  
  public ZqlParser(Reader in) {
      initParser(in);
  }

  /**
   * Create a new parser: before use, call initParser(InputStream) to
   * specify an input stream for the parsing.
   */
  public ZqlParser() {};

  /**
   * Initialize (or re-initialize) the input stream for the parser.
   */
  public void initParser(InputStream in) {
    if(_parser == null) {
      _parser = new ZqlJJParser(in);
    } else {
      _parser.ReInit(in);
    }
  }
  
  public void initParser(Reader in) {
      if (_parser == null) {
          _parser = new ZqlJJParser(in);
      } else {
          _parser.ReInit(in);
      }
  }

  public void addCustomFunction(String fct, int nparm) {
    ZUtils.addCustomFunction(fct, nparm);
  }

  /**
   * Parse an SQL Statement from the parser's input stream.
   * @return An SQL statement, or null if there's no more statement.
   */
  public ZStatement readStatement() throws ParseException {
    if(_parser == null)
      throw new ParseException(
       "Parser not initialized: use initParser(InputStream);");
    return _parser.SQLStatement();
  }

  /**
   * Parse a set of SQL Statements from the parser's input stream (all the
   * available statements are parsed and returned).
   * @return A vector of ZStatement objects (SQL statements).
   */
  public Vector<ZStatement> readStatements() throws ParseException {
    if(_parser == null)
      throw new ParseException(
       "Parser not initialized: use initParser(InputStream);");
    return _parser.SQLStatements();
  }

  /**
   * Parse an SQL Expression (like the WHERE clause of an SQL query).
   * @return An SQL expression.
   */
  public ZExp readExpression() throws ParseException {
    if(_parser == null)
      throw new ParseException(
       "Parser not initialized: use initParser(InputStream);");
    return _parser.SQLExpression();
  }

};

