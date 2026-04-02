package edu.upenn.cis.orchestra.datamodel;

import edu.upenn.cis.orchestra.datamodel.Atom.AtomType;

public class TypedRelation {
	public Relation rel;
	public AtomType typ;

	public TypedRelation(Relation r, AtomType t) {
		rel = r;
		typ = t;
	}

	public boolean equals(TypedRelation other){
		return (rel.equals(other.rel) && typ.equals(other.typ));
	}

	public boolean equals(Object o){
		if(o instanceof TypedRelation) {
			return equals((TypedRelation)o);
		}else{
			return false;
		}
	}

	public int hashCode(){
		return rel.getName().hashCode();
	}
}


