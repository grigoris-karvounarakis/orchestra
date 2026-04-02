package edu.upenn.cis.orchestra.exchange.tukwila;

import java.util.List;

import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.dbms.IDb;
import edu.upenn.cis.orchestra.dbms.TukwilaDb;
import edu.upenn.cis.orchestra.exchange.CreateProvenanceStorage;
import edu.upenn.cis.orchestra.exchange.OuterUnionMapping;
import edu.upenn.cis.orchestra.provenance.ProvenanceRelation;

public class CreateProvenanceStorageTukwila extends CreateProvenanceStorage {

	protected TukwilaDb _db;

	public CreateProvenanceStorageTukwila(TukwilaDb db) {
		_db = db;
	}

	@Override
	public void createOuterUnionDbTable(ProvenanceRelation rel,
			boolean withLogging, IDb db) {
		
		rel.getPrimaryKey().getFields();
	}

	@Override
	public void createProvenanceDbTable(final Relation rel, boolean withNoLogging, IDb db) 
	{
		// TODO Auto-generated method stub
	}

}
