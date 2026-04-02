CONNECT TO OWLDB@

CREATE FUNCTION GetId(namespace varchar(250), localname varchar(25))
     RETURNS INTEGER
     LANGUAGE SQL
	READS SQL DATA
     DETERMINISTIC
BEGIN ATOMIC
	declare namesp VARCHAR(250);
	set namesp=namespace;
	IF namespace='owl' THEN set namesp='http://www.w3.org/2002/07/owl#';
	ELSE IF namespace='rdf' THEN set namesp='http://www.w3.org/1999/02/22-rdf-syntax-ns#';
	ELSE IF namespace='rdfs' THEN set namesp='http://www.w3.org/2000/01/rdf-schema#';
	END IF;
	END IF;
	END IF;
	return (SELECT resources.id FROM resources INNER JOIN ns ON(resources.ns=ns.id) WHERE ns.ns=namesp AND name=localname);
END@