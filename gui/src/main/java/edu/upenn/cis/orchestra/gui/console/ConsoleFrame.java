package edu.upenn.cis.orchestra.gui.console;

import java.awt.BorderLayout;
import java.io.IOException;

import javax.swing.JComboBox;
import javax.swing.JInternalFrame;
import javax.swing.JTextArea;

import edu.upenn.cis.orchestra.console.Console;
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;

public class ConsoleFrame extends JInternalFrame {
    JTextArea m_textArea = new JTextArea();
    JComboBox m_combobox = new JComboBox();
    Console m_console;

    static final long serialVersionUID = 42;
    
    public ConsoleFrame(OrchestraSystem peers, String cdssName) throws IOException {
    	super (cdssName + " console", true, false, true, true);

        getContentPane().add(new ConsolePanel(peers), BorderLayout.CENTER);
        pack ();
    }
}