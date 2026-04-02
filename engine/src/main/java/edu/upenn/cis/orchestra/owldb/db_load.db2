CONNECT TO OWLDB;

LOAD FROM 'owldb_literals.csv' OF DEL INSERT INTO literals;
LOAD FROM 'owldb_resources.csv' OF DEL INSERT INTO resources;
LOAD FROM 'owldb_namespaces.csv' OF DEL INSERT INTO ns;
LOAD FROM 'owldb_statements.csv' OF DEL INSERT INTO statements;

INSERT INTO lists WITH tpt(list,subject,member) AS (
	SELECT subject,subject,object FROM statements WHERE predicate=getID('rdf','first') AND
		subject IN (SELECT object FROM statements WHERE predicate=getID('owl','oneOf') OR predicate=getID('owl','intersectionOf') OR predicate=getID('owl','unionOf'))
			UNION ALL
			SELECT list,s2.subject,s2.object FROM tpt,statements s1,statements s2
				WHERE tpt.subject=s1.subject AND s1.predicate=getID('rdf','rest') AND s1.object=s2.subject
					AND s2.predicate=getID('rdf','first')
		) SELECT list,member,'r' FROM tpt;

INSERT INTO restrictions SELECT s1.subject,s1.predicate,s2.object property,
CASE WHEN s1.object_is='l' THEN (SELECT CAST(value AS INT) FROM literals WHERE id=s1.object) ELSE s1.object END value
	FROM statements s1 INNER JOIN statements s2 ON(s1.subject=s2.subject)
	WHERE (s1.predicate=getId('owl','someValuesFrom') OR s1.predicate=getId('owl','allValuesFrom') OR
			s1.predicate=getId('owl','hasValue') OR s1.predicate=getId('owl','minCardinality') OR
			s1.predicate=getId('owl','maxCardinality') OR s1.predicate=getId('owl','cardinality')
		) AND s2.predicate=getId('owl','onProperty');
