
CREATE TABLE peer 
(
 peerId VARCHAR( 80 ),
 address VARCHAR( 1000 ),
 description VARCHAR( 3000 ),
 CONSTRAINT peer_ck_peerId_nn CHECK (peerId IS NOT NULL),
 CONSTRAINT peer_ck_address_nn CHECK (address IS NOT NULL),
 CONSTRAINT peer_pk PRIMARY KEY (peerId)
) ENGINE = innodb COMMENT = 'Orchestra peers';

CREATE TABLE orchSchema (
peerId VARCHAR( 80 ),
schemaId VARCHAR( 80 ),
description VARCHAR( 400 ),
CONSTRAINT orchSchema_ck_peerId_nn CHECK (peerId IS NOT NULL),
CONSTRAINT orchSchema_ck_schemaId_nn CHECK (schemaId IS NOT NULL),
CONSTRAINT orchSchema_ck_description_nn CHECK (description IS NOT NULL),
CONSTRAINT orchSchema_pk PRIMARY KEY (peerId, schemaId),
CONSTRAINT orchSchema_fk_peer FOREIGN KEY (peerId) REFERENCES peer (peerId)
) ENGINE = innodb COMMENT = 'Lists the schemas';


CREATE TABLE relations (
peerId VARCHAR( 80 ),
schemaId VARCHAR( 80 ),
dbSchema VARCHAR( 80 ),
dbCatalog VARCHAR( 80 ),
dbRelName    VARCHAR (80),
name VARCHAR( 80 ),
description VARCHAR( 400 ),
materialized BOOL,
statNbRows INT,
CONSTRAINT relations_ck_peerId_nn CHECK (peerId IS NOT NULL),
CONSTRAINT relations_ck_schemaId_nn CHECK (schemaId IS NOT NULL),
CONSTRAINT relations_ck_dbRelName_nn CHECK (dbRelName IS NOT NULL),
CONSTRAINT relations_ck_name_nn CHECK (name IS NOT NULL),
CONSTRAINT relations_ck_description_nn CHECK (description IS NOT NULL),
CONSTRAINT relations_ck_materialized_nn CHECK (materialized IS NOT NULL),
CONSTRAINT relations_pk PRIMARY KEY (peerId, schemaId, name),
CONSTRAINT relations_fk_schema FOREIGN KEY (peerId, schemaId ) REFERENCES orchSchema ( peerId, schemaId )
) ENGINE = innodb COMMENT = 'Lists the schemas';



CREATE TABLE fields (
peerId VARCHAR( 80 ),
schemaId VARCHAR( 80 ),
relationId VARCHAR( 80 ),
name VARCHAR( 80 ),
type INT,
dbType VARCHAR (100),
dbSize INT,
description VARCHAR( 400 ),
isNullable BOOL,
CONSTRAINT fields_ck_peerId_nn CHECK (peerId IS NOT NULL),
CONSTRAINT fields_ck_schemaId_nn CHECK (schemaId IS NOT NULL),
CONSTRAINT fields_ck_relationId_nn CHECK (relationId IS NOT NULL),
CONSTRAINT fields_ck_name_nn CHECK (name IS NOT NULL),
CONSTRAINT fields_ck_type_nn CHECK (type IS NOT NULL),
CONSTRAINT fields_ck_dbType_nn CHECK (dbType IS NOT NULL),
CONSTRAINT fields_ck_dbSize_nn CHECK (dbSize IS NOT NULL),
CONSTRAINT fields_ck_description_nn CHECK (description IS NOT NULL),
CONSTRAINT fields_ck_isNullable_nn CHECK (isNullable IS NOT NULL),
CONSTRAINT fields_pk PRIMARY KEY (peerId, schemaId, relationId, name),
CONSTRAINT fields_fk_relations FOREIGN KEY (peerId, schemaId, relationId) 
				REFERENCES relations (peerId, schemaId, name)
) ENGINE = innodb COMMENT = 'Contains a relation fields';

CREATE TABLE constraints (
peerId VARCHAR( 80 ),
schemaId VARCHAR( 80 ),
relationId VARCHAR( 80 ),
name VARCHAR( 80 ),
typeCst CHAR( 1 ),
statNbUniqueVals INT,
CONSTRAINT constraints_ck_peerId_nn CHECK (peerId IS NOT NULL),
CONSTRAINT constraints_ck_schemaId_nn CHECK (schemaId IS NOT NULL),
CONSTRAINT constraints_ck_relationId_nn CHECK (relationId IS NOT NULL),
CONSTRAINT constraints_ck_name_nn CHECK (name IS NOT NULL),
CONSTRAINT constraints_ck_typeCst_nn CHECK (typeCst IS NOT NULL),
CONSTRAINT constraints_PK PRIMARY KEY ( peerId, schemaId , relationId , name ),
CONSTRAINT constrains_chk_type CHECK (typeCst in ('P','F','U', 'I')),
CONSTRAINT constraints_fk_relations FOREIGN KEY (peerId, schemaId, relationId) 
				REFERENCES relations (peerId, schemaId, name)
) ENGINE = innodb;

CREATE TABLE constraintsFields (
peerId VARCHAR( 80 ),
schemaId VARCHAR( 80 ),
relationId VARCHAR( 80 ),
name VARCHAR( 80 ),
fldName VARCHAR( 80 ),
fldPosition INT,
CONSTRAINT constraints_ck_peerId_nn CHECK (peerId IS NOT NULL),
CONSTRAINT constraints_ck_schemaId_nn CHECK (schemaId IS NOT NULL),
CONSTRAINT constraints_ck_relationId_nn CHECK (relationId IS NOT NULL),
CONSTRAINT constraints_ck_name_nn CHECK (name IS NOT NULL),
CONSTRAINT constraints_ck_fldName_nn CHECK (fldName IS NOT NULL),
CONSTRAINT constraints_ck_fldPosition_nn CHECK (fldPosition IS NOT NULL),
CONSTRAINT constraintsFields_PK PRIMARY KEY ( peerId, schemaId , relationId , name , fldName),
CONSTRAINT constraintsFields_fk_constraints FOREIGN KEY (peerId, schemaId , relationId , name) 
						REFERENCES constraints (peerId, schemaId , relationId , name),
CONSTRAINT constraintsFields_fk_fields	FOREIGN KEY (peerId, schemaId, relationId, fldName)
						REFERENCES fields (peerId, schemaId, relationId, name)					
) ENGINE = innodb;

CREATE TABLE foreignKeys (
peerId VARCHAR( 80 ),
schemaId VARCHAR( 80 ) NOT NULL ,
relationId VARCHAR( 80 ) NOT NULL ,
name VARCHAR( 80 ) NOT NULL ,
refRelationId VARCHAR (80) NOT NULL,
refFldName VARCHAR( 80 ) NOT NULL ,
refFldPosition INT NOT NULL,
CONSTRAINT constraints_ck_peerId_nn CHECK (peerId IS NOT NULL),
CONSTRAINT constraints_ck_schemaId_nn CHECK (schemaId IS NOT NULL),
CONSTRAINT constraints_ck_relationId_nn CHECK (relationId IS NOT NULL),
CONSTRAINT constraints_ck_name_nn CHECK (name IS NOT NULL),
CONSTRAINT constraints_ck_refRelationId_nn CHECK (refRelationId IS NOT NULL),
CONSTRAINT constraints_ck_refFldName_nn CHECK (refFldName IS NOT NULL),
CONSTRAINT constraints_ck_refFldPosition_nn CHECK (refFldPosition IS NOT NULL),
CONSTRAINT foreignKeys_PK PRIMARY KEY ( peerId, schemaId , relationId , name , refRelationId, refFldName),
CONSTRAINT foreignKeys_fk_constraints FOREIGN KEY (peerId, schemaId , relationId , name) 
						REFERENCES constraints (peerId, schemaId , relationId , name),
CONSTRAINT foreignKeys_fk_fields	FOREIGN KEY (peerId, schemaId, refRelationId, refFldName)
						REFERENCES fields (peerId, schemaId, relationId, name)					
) ENGINE = innodb;


CREATE TABLE mappings (
  peerId			VARCHAR(80),
  mappingId   		VARCHAR(80),
  description       VARCHAR(400),
  isMaterialized    BOOL,
  trustRank			INT(10),
  CONSTRAINT mappings_ck_mappingId_nn CHECK (mappingId IS NOT NULL),
  CONSTRAINT mappings_ck_description_nn  CHECK (description IS NOT NULL),
  CONSTRAINT mappings_ck_isMaterialized_nn CHECK (isMaterialized IS NOT NULL),
  CONSTRAINT mappings_ck_trustRank_nn CHECK (trustRank IS NOT NULL),
  CONSTRAINT mappings_ck_peerId_nn CHECK (peerId IS NOT NULL),
  CONSTRAINT mappings_pk PRIMARY KEY (peerId, mappingId),
  CONSTRAINT mappings_fk_peer FOREIGN KEY (peerId) REFERENCES peer (peerId)
) ENGINE = innodb;


CREATE TABLE mappingsAtoms (
  peerId             VARCHAR (80),
  mappingId    		 VARCHAR (80),
  isHead			 BOOL,
  atomPeerId         VARCHAR (80),
  atomSchemaId		 VARCHAR (80),
  atomRelationId     VARCHAR (80),
  atomOrder			 INT(5),
  atomValues		 VARCHAR(1000),
  CONSTRAINT mappingsAtoms_ck_peerId_nn CHECK (peerId IS NOT NULL),
  CONSTRAINT mappingsAtoms_ck_mappingId_nn CHECK (mappingId IS NOT NULL),  
  CONSTRAINT mappingsAtoms_ck_atomPeerId_nn CHECK (atomPeerId IS NOT NULL),
  CONSTRAINT mappingsAtoms_ck_atomSchemaId_nn CHECK (atomSchemaId IS NOT NULL),
  CONSTRAINT mappingsAtoms_ck_atomRelationId_nn CHECK (atomRelationId IS NOT NULL),
  CONSTRAINT mappingsAtoms_ck_atomOrder_nn CHECK (atomOrder IS NOT NULL),
  CONSTRAINT mappingsAtoms_ck_atomValues_nn CHECK (atomValues IS NOT NULL),
  CONSTRAINT mappingsAtoms_fk_mappings FOREIGN KEY (peerId, mappingId) REFERENCES mappings(peerId, mappingId)
) ENGINE = innodb;

CREATE INDEX mappingAtoms_atomIdx ON mappingsAtoms (atomPeerId, atomSchemaId, atomRelationId);

CREATE INDEX mappingsAtoms_mappingIdx ON mappingsAtoms (peerId, mappingId);
