Load DATA
INFILE '../distrib/catalog/catalog.dmp'
BADFILE 'log/catalog.bad'
INTO TABLE catalog
  FIELDS TERMINATED BY  '\t' 
	TRAILING NULLCOLS
    ( 
		   tax_id,
		   species_name,
		   accession,
		   gi,
		   releaseDirectory CHAR(3000),
		   status,
		   length
    	)