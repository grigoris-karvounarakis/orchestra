#!/sw/bin/python

"""Synthetic workload generator for SIGMOD07 experiments.

Usage: workload.py [options]

OPTIONS 

-?          --help
            Display this usage message.
-a [N]      --skip=[N]
            Skip the first N Swissprot entries (default is 0)
-b          --bidir
            Generate bidirectional mappings
-c [N]      --cutoff=[N]
            Set maximum VARCHAR size in generated schemas (default is
            1024).
-d [N]      --deletions=[N]
            Set deletion load as measured in number of Swissprot entries
            per peer to be deleted (default is 1).  The deletion load may
            be no greater than the insertion load.
-e          --oraclemode
            Output scripts for Oracle rather than DB2
-f [prefix] --filename=[prefix]
            Set filename prefix to use for output files.  If un-
            specified, the program writes to standard output.
-h          --xmlformat
            Output schema file in XML format
-i [N]      --insertions=[N]
            Set insertion load as measured in number of Swissprot entries
            per peer to be inserted (default is 1).
-j          Use tukwila engine (default is DB2)
-k [N]      --maxcycles=[N]
            Set maximum number of simple cycles (default is no maximum).
-l [N]      --mincycles=[N]
            Set minimum number of simple cycles (default is no minimum).
-m [N]      --relsize
            Set standard relation size -- number of attributes -- in peer relations
            (default is 4).  All but one relation in each peer will have exactly this
             many attributes (the other will have at most this many).
-n [N]      --fanout=[N]
            Set maximum fanout from a peer in the graph of mappings
            (default is 1).
-o          --olivier
            Suppress CREATE TABLE commands for the _INS and _DEL relations
-p [N]      --peers=[N]
            Set number of peers (default is 3).
-r [N]      --seed=[N]
            Set random number generator seed (default is 0).
-s [N]      --schemas=[N]
            Set number of logical schemas (default is number of
            logical peers).
-t          --todd
            Hackalicious goodness, custom-pimped for the us-versus-DRed
            SIGMOD07 smackdown: 3 peers, total mapping P1->P3, P2->P3, P1
            and P2 get same entries, deletions: 100% from P1, _kdeletions
            from P2, none from P3
-u          --integers
            Use integers instead of strings in workload, with values obtained by
            hashing input strings.
-w [alias]  --updateAlias
            Set database-alias for secondary update store (default None)
-v [N]      --coverage=[N]
            Set average fraction of full set of attributes contained by a peer
            (default is 0.75).  Set to 1 to ensure all peers have all attributes.
-x [alias]  --dbalias [alias]
            Set database-alias for CONNECT TO (default 'DEFEAT').
-y [user]   --user [name]
            Set username for CONNECT TO (default 'TJGREEN').
-z [passwd] --password [passwd]
            Set password for CONNECT TO (default 'todd9807').

Each randomly generated schema is over the attributes of the Swissprot
database:

ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/
knowledgebase/complete/uniprot_sprot.dat.gz

The data used to initially populate peer instances and subsequent
updates is also taken from the Swissprot database.
"""

import copy, ftplib, getopt, os, random, re, string, sys, time
from string import join
import xml.dom

_kintegers = "integers"
_koracle = "oracle"
_kskip = "skip"
_kcoverage = "coverage"
_kcutoff = "cutoff"
_kinsertions = "insertions"
_kfilename = "filename"
_kfanout = "fanout"
_kxmlformat = "xmlformat"
_ktukwila = "tukwila"
_kolivier = "olivier"
_kmaxcycles = "maxcycles"
_kmincycles = "mincycles"
_krelsize = "relsize"
_kpeers = "peers"
_kschemas = "schemas"
_kseed = "seed"
_kdeletions = "deletions"
_kdbalias = "dbalias"
_kupdateAlias = "updateAlias"
_kusername = "username"
_kpassword = "password"
_khelp = "help"
_ktodd = "todd"
_kbidir = "bidir"

_flags = [("?", _khelp),
          ("e", _koracle),
          ("h", _kxmlformat),
          ("j", _ktukwila),
          ("t", _ktodd),
          ("b", _kbidir),
          ("u", _kintegers),
          ("o", _kolivier)]

_grigoris = True
_inout = True

_values = [("a", _kskip, 0),
           ("c", _kcutoff, 1024),
           ("i", _kinsertions, 1),
           ("f", _kfilename, None),
           ("k", _kmaxcycles, None),
           ("l", _kmincycles, None),
           ("m", _krelsize, 4),
           ("n", _kfanout, 2),
           ("p", _kpeers, 3),
           ("r", _kseed, 0),
           ("s", _kschemas, None),
           ("d", _kdeletions, 1),
           ("w", _kupdateAlias, None),
           ("v", _kcoverage, 0.75),
           ("x", _kdbalias, "DEFEAT"),
           ("y", _kusername, "TJGREEN"),
           ("z", _kpassword, "todd9807")]

_shortnames = map(lambda(a,b) : a, _flags) + map(lambda (a,b,c) : a, _values)
_longnames = map(lambda(a,b) : b, _flags) + map(lambda(a,b,c) : b, _values)
_params = dict(map(lambda(a,b) : (b,False), _flags) + map(lambda(a,b,c) : (b,c), _values))

_stats = {'FT': 38278, 'DE': 795, 'DT': 115, 'DR': 24337, 'RT': 18683,
'RP': 7791, 'RX': 7309, 'RG': 259, 'RA': 13234, 'RC': 1827, 'RL':
5857, 'RN': 821, 'AC': 911, 'CC': 13160, 'GN': 1061, 'ID': 44, 'OH':
1230, 'OG': 385, 'SQ': 58, 'OC': 281, 'KW': 585, 'OX': 18, 'OS': 144,
'SD': 37784, 'PE': 20000}

_atts = _stats.keys()

_INS = "_INS"
_DEL = "_DEL"
_L = "_L"
_L_INS = "_L_INS"
_L_DEL = "_L_DEL"
_R = "_R"

_schemas = []    # _schemas[i] is a list of lists of attribute names
_peers = []      # _peers[i] is an index into the schemas list
_mappings = []   # _mappings[i] is a triple (i, j, X) where i, j are 
                 # peers and X is a list of attributes
_cycles = []

def find_simple_cycles():
    global _cycles
    # First, index the edges.
    edges = []
    for i in xrange(len(_peers)):
        edges.append([])
    for (i,j,X) in _mappings:
        edges[i].append(j)
    for i in xrange(len(_peers)):
        edges[i].sort
    # Find simple cycles as follows:
    # - Handle the peers in order
    # - Find simple cycles where the smallest node in the cycle
    #   is the peer
    _cycles = []
    for i in xrange(len(_peers)):
        paths = [[i]]
        while len(paths) != 0:
            path = paths.pop()
            for j in edges[path[len(path)-1]]:
                if j == i:
                    _cycles.append(path + [j])
                elif j > i and j not in path:
                    paths.append(path + [j])

def spart(j):
    if _grigoris:
        return "S" + str(j) + "_"
    else:
        return ""

def ppart(i):
    return "P" + str(i) + "_"

def rpart(k):
    return "R" + str(k)

class MyException(Exception):
    pass

def suffixes():
    if _params[_kolivier] and _inout:
        return [ "", _L, _R ]
    elif _params[_kolivier]:
        return [ "" ]
    elif _inout:
        return [ "", _INS, _DEL, _L, _L_INS, _L_DEL, _R ]
    else:
        return [ "", _INS, _DEL ]

def _ins():
    if _inout:
        return _L_INS
    else:
        return _INS

def _del():
    if _inout:
        return _L_DEL
    else:
        return _DEL

def zipfdraw(s, N):
    denom = sum(map(lambda n : 1.0/n**s, range(1,N+1)))
    density = map(lambda k : (1.0/k**s)/denom, range(1,N+1))
    law = map(lambda k : sum(density[0:k-1]), range(1,N+1))
    sample = random.random()
    cum = 0
    for i in range(0,N):
        cum = cum + density[i]
        if cum >= sample:
            return i
    return N-1             # unreachable except for rounding

def subset(n):
    subset = []
    unused = _atts[:]
    for i in range(0, n):
        att = random.choice(unused)
        subset.append(att)
        unused.remove(att)
    for att in unused:
        if random.choice([True, False]):
            subset.append(att)
    return subset

def indexmap(f, a):
    b = []
    for i in range(0, len(a)):
        b.append(f(i,a[i]))
    return b

def selectatoms(i, key, X, ch):
    X = set(X)
    atoms = []
    for k in range(0, len(_schemas[_peers[i]])):
        rel = _schemas[_peers[i]][k]
        if len(set(rel).intersection(X)) != 0:
            atts = [key]
            for att in rel:
                if att in X:
                    atts.append(att)
                else:
                    atts.append(ch)
            if _params[_kxmlformat]:
                atoms.append("P" + str(i) + ".S" + str(_peers[i])+ "." + printrel(i,_peers[i],k,atts))
            else:
                atoms.append("P.S." + printrel(i,_peers[i],k,atts))
    return atoms

def generate():
    global _schemas, _peers, _mappings
    
    # generate the random schemas
    _schemas = []
    for i in range(0, _params[_kschemas]):
        pi = _atts[:]
        random.shuffle(pi)
        schema = []
        # fixed-size relations
        j = 0
        schema.append([])
        for att in pi:
            if random.random() <= _params[_kcoverage]:
                if len(schema[j]) == _params[_krelsize] - 1:
                    j = j + 1
                    schema.append([])
                schema[j].append(att)
        _schemas.append(schema)

    # assign peers to schemas
    _peers = range(0, _params[_kschemas])
    for i in range(_params[_kschemas], _params[_kpeers]):
        j = random.randint(1, len(_schemas))
        _peers.append(j-1)

    # generate mappings among the peers
    _mappings = []
    if _params[_ktodd]:
        # Billy Joel says waiting too long can give you a
        # heart a-hack hack hack hack hack hack
        # P1 -> P3, P2 -> P3, P3 -> P4, P4 -> ... P5 -> PN
        # _mappings.append((0,2,_atts))
        # _mappings.append((1,2,_atts))
        # _mappings.append((3,2,_atts))
        # for j in range(3, _params[_kpeers]-1):
        #    _mappings.append((j, j+1, _atts))
        # (Grigoris) Changed to: P1 -> P3, P3 -> P5, 
        # P2 -> P4, P4 -> P5, P5 -> P6 -> ... -> PN
        _mappings.append((0,2,_atts))
        _mappings.append((2,4,_atts))
        _mappings.append((1,3,_atts))
        _mappings.append((3,4,_atts))
        #_mappings.append((5,4,_atts))
        for j in range(4, _params[_kpeers]-1):
           _mappings.append((j, j+1, _atts))
        #for j in range(5, _params[_kpeers]-1):
        #   if (j % 3 == 0):
        #     _mappings.append((j, j+1, _atts))
        #   else:
        #     _mappings.append((j+1, j, _atts))

    else:
   
     # First add mappings to create ensure all peers are
        # connected (connected in an undirected sense).  The
        # resulting graph of mappings is always acyclic.
        allAvail = range(len(_peers))
        for i in range(len(_peers)):
            avail = allAvail[i] = range(len(_peers))
            avail.remove(i)
            if i != 0:
                j = random.choice(range(i))
                if random.choice([True,False]):
                    addMapping(i, j, avail)
                else:
                    addMapping(j, i, allAvail[j])
        # Then add more mappings according to the fanout parameter.
        # These mappings may introduce cycles.
        for i in range(len(_peers)):
            avail = allAvail[i]
            for j in range(_params[_kfanout]):
                if len(avail) != 0 and random.choice([True,False]):
                    k = random.randrange(0, len(avail))
                    # generate a mapping from i to avail[k]
                    # based on common attributes
                    addMapping(i, avail[k], avail)

def addMapping(i, j, avail):
    s1 = set(unnest(_schemas[i]))
    s2 = set(unnest(_schemas[j]))
    mapping = (i, j, list(s1.intersection(s2)))
    _mappings.append(mapping)
    avail.remove(j)
    
def unnest(a):
    b = []
    for n in a:
        b = b + n
    return b
    

def matchname(s):
    for i in range(0, len(_shortnames)):
        if s in ("-" + _shortnames[i], "--" + _longnames[i]):
            return _longnames[i]
    assert False

def nextentry(f):
    # parses the next entry from the swissprot flat file
    entry = {}
    while True:
        line = f.readline()
        if line == "" or line.startswith("//"):
            break
        key = line[0:2]
        data = line[5:].strip()
        if key == "  ":
            key = "SD"  # sequence data
        if entry.get(key):
            data = entry.get(key) + " " + data
        maxlen = min(_params[_kcutoff], _stats[key])
        entry[key] = data[:maxlen]
    if entry == {}:
        return None
    return entry

def escape(s):
    return s.replace("|", ":").replace('"','_')

def writeentry(files, entry, suffix, i, key):
    # populate each relation in peer i
    j = _peers[i]
    for k in range(0, len(_schemas[j])):
        vals = [ str(key) ]
        for att in _schemas[j][k]:
            if entry.get(att):
                if _params[_kintegers]:
                    vals.append(abs(hash(entry[att])))
                else:
                    vals.append(escape(str(entry[att])))
            else:
                vals.append("null")
        files[k].write(join(map(str, vals), "|") + "\n")

def bulk_open(file, i, suffix):
    if _params[_koracle]:
        return bulk_open_oracle(file, i, suffix)
    else:
        return bulk_open_db2(file, i, suffix)

def bulk_open_oracle(file, i, suffix):
    a = []
    j = _peers[i]
    for k in range(0, len(_schemas[j])):
        name = relname(i,j,k) + suffix
        if _params[_kfilename] == None:
            a.append(sys.stdout)
        else:
            a.append(outopen(name))
        if _params[_kfilename] == None:
            fname = ""
        else:
            fname = _params[_kfilename] + "."
        tablename = relname(i,j,k) + suffix
        file.write("sqlldr " + _params[_kusername] + "/" + _params[_kpassword] +
                   "@" + _params[_kdbalias] + " CONTROL=" + fname + tablename + ".ctl" + " LOG=" + fname + tablename + ".log" + "\n")
        if _params[_kintegers]:
            typed = map(lambda x : x + " INTEGER", _schemas[j][k])
        else:
            typed = map(lambda x : x + " CHAR(" + str(_params[_kcutoff]) + ")", _schemas[j][k])
        atts = join(["KID"] + typed, ", ")
        ctlfile = outopen(tablename + ".ctl")
        ctlfile.write("Load DATA\n")
        ctlfile.write("INFILE '" + fname + tablename + "'\n")
        ctlfile.write("BADFILE '" + fname + tablename + ".bad'\n")
        ctlfile.write("INTO TABLE " + tablename + "\n")
        ctlfile.write("FIELDS TERMINATED BY '|'\n")
        ctlfile.write("TRAILING NULLCOLS\n")
        ctlfile.write("(" + atts + ")\n\n")
        ctlfile.close()
    return a

def bulk_open_db2(file, i, suffix):
    a = []
    j = _peers[i]
    for k in range(0, len(_schemas[j])):
        name = relname(i,j,k) + suffix
        if _params[_kfilename] == None:
            a.append(sys.stdout)
        else:
            a.append(outopen(name))
        if _params[_kfilename] == None:
            fname = ""
        else:
            fname = _params[_kfilename] + "."
        tablename = relname(i,j,k) + suffix
        atts = join(["KID"] + _schemas[j][k], ", ")
        file.write("IMPORT FROM " + fname + tablename)
        ran = map(str, range(1, 2 + len(_schemas[j][k])))
        file.write(" OF DEL MODIFIED BY COLDEL| METHOD P (" + join(ran, ", ") + ") MESSAGES NUL")
        file.write(" INSERT INTO " + tablename + "(" + atts + ");\n")
        file.write("RUNSTATS ON TABLE " + _params[_kusername] + "." + tablename + " ON ALL COLUMNS ALLOW WRITE ACCESS;\n")
    return a

def bulk_close(a):
    if _params[_kfilename] != None:
        for f in a:
            f.close()
    a[:] = []   # clear a

def fillheader(file):
    if _params[_koracle]:
        file.write("#!/usr/bin/sh\n#\n")
        file.write("# " + stamp() + " " + str(_params) + "\n#\n")
    else:
        header(file)
        
def fill(iout, dout):
    fillheader(iout)
    fillheader(dout)

    f = open("uniprot_sprot.dat", "r")

    # skip the first _params[_kskip] entries
    key = _params[_kskip]
    for i in range(0, _params[_kskip]):
        nextentry(f)

    if _params[_ktodd]:
        # MC Hammer says stop now, hacker time!        
        # Make P1 and P2 have the same insertions, P1 have 100%
        # deletions, P2 have _params[_kdeletions] deletions
 
        # (Grigoris) Changed to: P2 have _params[_kdeletions] 
        # deletions, P1 has none
        p1_ins = bulk_open(iout, 0, _ins())
        p1_del = bulk_open(dout, 0, _del())
        p2_ins = bulk_open(iout, 1, _ins())
        p2_del = bulk_open(dout, 1, _del())
        p3_ins = bulk_open(iout, 2, _ins())
        p4_ins = bulk_open(iout, 3, _ins())
        p4_del = bulk_open(dout, 3, _del())
        p5_ins = bulk_open(iout, 4, _ins())
#        p6_ins = bulk_open(iout, 5, _ins())
#        p7_ins = bulk_open(iout, 6, _ins())
#        p8_ins = bulk_open(iout, 7, _ins())
#        p9_ins = bulk_open(iout, 8, _ins())
#        p10_ins = bulk_open(iout, 9, _ins())

        # populate each peer with datasize entries
        updates = 0
        half = 0
        for d in range(0, _params[_kinsertions]):
            # grab the next entry from swissprot
            entry = nextentry(f)
            if entry == None:
                f.seek(0)
                entry = nextentry(f)
                assert entry != None
            assert len(entry.keys()) > 0
            writeentry(p1_ins, entry, _ins(), 0, key)
#            writeentry(p1_del, entry, _del(), 0, key)
            writeentry(p2_ins, entry, _ins(), 1, key)
            writeentry(p3_ins, entry, _ins(), 2, key)
            writeentry(p4_ins, entry, _ins(), 3, key)
            writeentry(p5_ins, entry, _ins(), 4, key)
#            writeentry(p6_ins, entry, _ins(), 5, key)
#            writeentry(p7_ins, entry, _ins(), 6, key)
#            writeentry(p8_ins, entry, _ins(), 7, key)
#            writeentry(p9_ins, entry, _ins(), 8, key)
#            writeentry(p10_ins, entry, _ins(), 9, key)
            if updates < _params[_kdeletions]:
                writeentry(p2_del, entry, _del(), 1, key)
                if half == 0:
                  writeentry(p1_del, entry, _del(), 0, key)
                  half = 1
                else:
                  writeentry(p4_del, entry, _del(), 3, key)
                  half = 0

                updates = updates + 1
            key = key + 1

        # end this dirty deed (done dirt cheap)
        bulk_close(p1_ins)
        bulk_close(p1_del)
        bulk_close(p2_ins)
        bulk_close(p2_del)
        bulk_close(p3_ins)
        bulk_close(p4_ins)
        bulk_close(p4_del)
        bulk_close(p5_ins)
#        bulk_close(p6_ins)
#        bulk_close(p7_ins)
#        bulk_close(p8_ins)
#        bulk_close(p9_ins)
#        bulk_close(p10_ins)

        # populate the peers one at a time
        for i in range(5, len(_peers)):

            bulk_ins = bulk_open(iout, i, _ins())
            bulk_del = bulk_open(dout, i, _del())

            # populate each peer with datasize entries
            updates = 0
            for d in range(0, _params[_kinsertions]):
                # grab the next entry from swissprot
                entry = nextentry(f)
                if entry == None:
                    f.seek(0)
                    entry = nextentry(f)
                    assert entry != None
                assert len(entry.keys()) > 0
                writeentry(bulk_ins, entry, _ins(), i, key)
                if updates < _params[_kdeletions]:
                    writeentry(bulk_del, entry, _del(), i, key)
                    updates = updates + 1
                key = key + 1

            bulk_close(bulk_ins)
            bulk_close(bulk_del)

    else:
        # populate the peers one at a time
        for i in range(0, len(_peers)):

            bulk_ins = bulk_open(iout, i, _ins())
            bulk_del = bulk_open(dout, i, _del())

            # populate each peer with datasize entries
            updates = 0
            for d in range(0, _params[_kinsertions]):
                # grab the next entry from swissprot
                entry = nextentry(f)
                if entry == None:
                    f.seek(0)
                    entry = nextentry(f)
                    assert entry != None
                assert len(entry.keys()) > 0
                writeentry(bulk_ins, entry, _ins(), i, key)
                if updates < _params[_kdeletions]:
                    writeentry(bulk_del, entry, _del(), i, key)
                    updates = updates + 1
                key = key + 1

            bulk_close(bulk_ins)
            bulk_close(bulk_del)
                

    f.close()

    footer(iout)
    footer(dout)

def relname(i,j,k):
    # peer i, schema j, relation k
    return ppart(i) + spart(j) + rpart(k)

def lrelname(i,j,k):
    return "P.S." + relname(i,j,k)

def printrel(i,j,k,a):
    return ppart(i) + printsrel(j,k,a)

def printpeer(p):
    list = []
    s = _schemas[_peers[p]]
    for j in range(0, len(s)):
        list.append(printrel(p, _peers[p], j, ["KID"] + s[j]))
    return "\n".join(list)

def printsrel(i,j,a):
    return spart(i) + rpart(j) + "(" + ", ".join(a) + ")"

def printschema(i,s):
    list = []
    for j in range(0, len(s)):
        list.append(printsrel(i, j, ["KID"] + s[j]))
    return "\n".join(list)

def universal_type(a):
    if _params[_kintegers]:
        return "integer"
    else:
        length = min(_stats[a], _params[_kcutoff])
        return "varchar(" + str(length) + ")"

def typed(a):
    if _params[_kintegers]:
        return a + " INTEGER INTEGER 10"
    else:
        length = min(_stats[a], _params[_kcutoff])
        return a + " VARCHAR " + " VARCHAR(" + str(length) + ") " + str(length)

def realtyped(a):
    if _params[_kintegers]:
        return a + " INTEGER"
    else:
        length = min(_stats[a], _params[_kcutoff])
        return a + " VARCHAR(" + str(length) + ")"

def create(file):
    header(file)
    for i in range(0, len(_peers)):
        j = _peers[i]
        for k in range(0, len(_schemas[j])):            
            for suffix in suffixes():
                file.write("CREATE TABLE " + relname(i,j,k) + suffix + " (\n")
                file.write(4*" " + "KID INTEGER NOT NULL,\n")
                atts = map(lambda a : 4*" " + realtyped(a), _schemas[j][k])
                file.write(join(atts, ",\n"))
                file.write("\n) " + noLoggingString() + ";\n")
#                file.write("CREATE INDEX IND_" + relname(i,j,k) + suffix + " ON " + relname(i,j,k) + suffix + "(KID) CLUSTER;\n")
    footer(file)
    if _params[_koracle]:
        file.write("EXIT\n")

def noLoggingString():
    if _params[_koracle]:
        return "NOLOGGING"
    else:
        return "NOT LOGGED INITIALLY"

def header(file):
    file.write("-- " + stamp() + " " + str(_params) + "\n")
    alias, user, pwd = _params[_kdbalias], _params[_kusername], _params[_kpassword]
    if not _params[_koracle]:
        file.write("CONNECT TO " + alias + " USER " + user + " USING \"" + pwd +
"\";\n")

def footer(file):
    if not _params[_koracle]:
        file.write("COMMIT WORK;\n")
        file.write("DISCONNECT ALL;\n")

def stamp():
    return time.strftime("%X %x %Z")

def destroy(file):
    header(file)
    for i in range(0, len(_peers)):
        for suffix in suffixes():
            j = _peers[i]
            for k in range(0, len(_schemas[j])):
                file.write("DROP TABLE " + relname(i,j,k) + suffix + ";\n")
    footer(file)

def iovariations():
    if _inout:
        return ["", _L, _R]
    else:
        return [""]

def addElement(doc, parent, label, name=None):
    child = doc.createElement(label)
    if name != None:
        child.setAttribute("name", name)
    parent.appendChild(child)
    return child

def metadata(file):
    if _params[_kxmlformat]:
        metadata_xml(file)
    else:
        metadata_old(file)

def metadata_xml(file):
    impl = xml.dom.getDOMImplementation()
    root = impl.createDocument(None, None, None)
    com = root.createComment(stamp() + "\n" + str(_params))
    root.appendChild(com)
    cat = addElement(root, root, "catalog")
    cat.setAttribute("recMode", "false")
    cat.setAttribute("name", _params[_kfilename])
    mig = addElement(root, cat, "migrated")
    m = root.createTextNode("true")
    mig.appendChild(m)
    for i in range(0, len(_peers)):
        peer = addElement(root, cat, "peer", "P" + str(i))
        peer.setAttribute("address", "grw561-3.cis.upenn.edu-7777")
        j = _peers[i]
        schema = addElement(root, peer, "schema", "S" + str(j))
        for k in range(0, len(_schemas[j])):
            for var in iovariations():
                name = relname(i,j,k) + var
                rel = addElement(root, schema, "relation", name)
                rel.setAttribute("materialized", "true")
                dbinfo = addElement(root, rel, "dbinfo")
#                dbinfo.setAttribute("catalog", "")
                dbinfo.setAttribute("schema", _params[_kusername])
                dbinfo.setAttribute("table", name)
                kid = addElement(root, rel, "field", "KID")
                kid.setAttribute("type", "integer")
                kid.setAttribute("key", "true")
                for att in _schemas[j][k]:
                    f = addElement(root, rel, "field", att)
                    f.setAttribute("type", universal_type(att))
    for k in range(0, len(_mappings)):
        (i,j,X) = _mappings[k]
        source = selectatoms(i, "KID", X, "_")  # _ means don't care
        target = selectatoms(j, "KID", X, "-")  # - means null
        m = addElement(root, cat, "mapping", "M" + str(k))
        m.setAttribute("materialized", "true")
        if _params[_kbidir]:
            m.setAttribute("bidirectional", "true")
        h = addElement(root, m, "head")
        for atom in target:
            a = addElement(root, h, "atom")
            if _params[_kbidir]:
                a.setAttribute("del", "true")
            t = root.createTextNode(atom)
            a.appendChild(t)
        b = addElement(root, m, "body")
        for atom in source:
            a = addElement(root, b, "atom")
            if _params[_kbidir]:
                b.setAttribute("del", "true")
            t = root.createTextNode(atom)
            a.appendChild(t)
    e = addElement(root, cat, "engine")
    t = addElement(root, e, "mappings")
    if _params[_ktukwila]:
        t.setAttribute("type", "tukwila")
        t.setAttribute("host", "grw561-3.cis.upenn.edu")
        t.setAttribute("port", "7777")
    else:
        t.setAttribute("type", "sql")
        t.setAttribute("server", "jdbc:db2://localhost:50000/" + _params[_kdbalias])
        t.setAttribute("username", _params[_kusername])
        t.setAttribute("password", _params[_kpassword])
    if _params[_kupdateAlias]:
        u = addElement(root, e, "updates")
        u.setAttribute("type", "sql")
        u.setAttribute("server", "jdbc:db2://localhost:50000/" + _params[_kupdateAlias])
        u.setAttribute("username", _params[_kusername])
        u.setAttribute("password", _params[_kpassword])
        
    # Output some default (dummy) reconciliation store info
    s = addElement(root, cat, "store")
    u = addElement(root, s, "update")
    u.setAttribute("type", "bdb")
    u.setAttribute("hostname", "localhost")
    u.setAttribute("port", "777")
    t = addElement(root, s, "state")
    t.setAttribute("type", "hash")
    # Output trust conditions saying that everyone trusts everyone
    for i in range(0, len(_peers)):
        j = _peers[i]
        tc = addElement(root, cat, "trustConditions")
        tc.setAttribute("peer", "P" + str(i))
        tc.setAttribute("schema", "S" + str(j))
        for i2 in range(0, len(_peers)):
            j2 = _peers[i2]
            if i != i2:
                for k2 in range(0, len(_schemas[j2])):
                    tr = addElement(root, tc, "trusts")
                    tr.setAttribute("pid", "P" + str(i2))
                    tr.setAttribute("pidType", "STRING")
                    tr.setAttribute("pidType", "STRING")
                    tr.setAttribute("priority", "5")
                    tr.setAttribute("relation", relname(i2,j2,k2))
    file.write(root.toprettyxml("  "))

def metadata_old(file):
    file.write("// " + stamp() + "\n")
    file.write("// " + str(_params) + """
PEERS
PEER P grw561-3.cis.upenn.edu-7777 "local peer"
    SCHEMAS
        SCHEMA S
            RELATIONS
""")
    for i in range(0, len(_peers)):
        j = _peers[i]
        for k in range(0, len(_schemas[j])):
            for var in iovariations():
                name = relname(i,j,k) + var
                file.write(16*" ")
                file.write("RELATION " + name + " MATERIALIZED\n")
                file.write(20*" ")
                file.write("DBINFO , " + _params[_kusername] + ", " + name + "\n")
                file.write(20*" ")
                fields = map(lambda a : typed(a), _schemas[j][k])
                file.write("FIELDS KID INTEGER INTEGER 10, " + join(fields, ", ") + "\n")
                file.write(20*" ")
                file.write("PRIMARY KEY PK_" + name + "(KID)\n")
    file.write("""        MAPPINGS
""")
    for k in range(0, len(_mappings)):
        (i,j,X) = _mappings[k]
        file.write(16*" ")
        source = selectatoms(i, "KID", X, "_")  # _ means don't care
        target = selectatoms(j, "KID", X, "-")  # - means null
        file.write("M" + str(k) + " MATERIALIZED: ");
        file.write(join(target, ", ") + " :- " + join(source, ", "))
        file.write("\n")

#     if _inout:
#         next = len(_mappings)
#         for i in range(0, len(_peers)):
#             j = _peers[i]
#             for k in range(0, len(_schemas[j])):
#                 atts = join(["KID"] + _schemas[j][k], ", ")
#                 name = "P.S." + relname(i,j,k)
#                 source = name + _L + "(" + atts + ")"
#                 target = name + "(" + atts + ")"
#                 file.write(16*" ")
#                 file.write("M" + str(next) + " MATERIALIZED: " + target + " :- " + source + "\n")
#                 next = next + 1

def cycles(file):
    file.write("// " + stamp() + "\n")
    file.write("// " + str(_params) + "\n")
    file.write("Simple cycle count: " + str(len(_cycles)) + "\n")
    file.write("Simple cycles: " + str(_cycles) + "\n")

def parse(a):
    if a == "":
        return True
    try:
        f = float(a)
        if f == float(int(f)):
            return int(f)
        return f
    except:
        return a

def outopen(name):
    return open(_params[_kfilename] + "." + name, "w")

def check(p1, p2, op):
    v1, v2 = _params[p1], _params[p2]
    if op == '>':
        flag = v1 > v2
    elif op == '>=':
        flag = v1 >= v2
    elif op == '<':
        flag = v1 < v2
    elif op == '<=':
        flag = v1 <= v2

    if flag == False:
        print "Error: you specified " + p1 + " = " + str(v1) + ", " + p2 + " = " + str(v2) + ",",
        print "but we require " + p1 + " " + op + " " + p2
        sys.exit(-1)

def main():
    # parse command line options
    try:
        st = map(lambda (a,b) : a, _flags) + map(lambda (a,b,c) : a + ":", _values)
        lg = map(lambda (a,b) : b, _flags) + map(lambda (a,b,c) : b + "=", _values)
        opts, args = getopt.getopt(sys.argv[1:], join(st, ""), lg)
    except getopt.error, msg:
        print msg
        print "for help use --help"
        sys.exit(2)
    # process options
    for o, a in opts:
        param = matchname(o)
        _params[param] = parse(a)

    # help?
    if _params[_khelp]:
        print __doc__
        sys.exit(0)

    # sanity-check the parameters
    check(_kpeers, _kschemas, ">=")
    check(_kpeers, _kfanout, ">")
    check(_kdeletions, _kinsertions, "<=")

    # number of schemas defaults to number of peers
    if _params[_kschemas] == None:
        _params[_kschemas] = _params[_kpeers]

    if _params[_kfilename] == None:
        mout = cout = iout = kout = dout = sout = sys.stdout
    else:
        names = ("schema", "create", "insert", "destroy", "delete", "cycles")
        (mout, cout, iout, kout, dout, sout) = map(outopen, names)

    # initialize the random number seed
    random.seed(_params[_kseed])

    # download and unzip the SWISSPROT data file if needed
    if not os.path.isfile("uniprot_sprot.dat"):
        if not os.path.isfile("uniprot_sprot.dat.gz"):
            print "Downloading uniprot_sprot.dat.gz ..."
            ftp = ftplib.FTP("ftp.uniprot.org")
            ftp.login()
            ftp.cwd("/pub/databases/uniprot/current_release/knowledgebase/complete")
            ftp.retrbinary('RETR uniprot_sprot.dat.gz', open('uniprot_sprot.dat.gz', 'wb').write)
            ftp.quit()
        print "Unzipping uniprot_sprot.dat ..."
        os.system("gunzip uniprot_sprot.dat.gz")

    # generate schemas and mappings
    while True:
        generate()
        find_simple_cycles()
        count = len(_cycles)
        if _params[_kmincycles] == None or count >= _params[_kmincycles]:
            if _params[_kmaxcycles] == None or count <= _params[_kmaxcycles]:
                break

    # print schemas, mappings, and data
    metadata(mout)
    create(cout)
    fill(iout, dout)
    destroy(kout)
    cycles(sout)
    
if __name__ == "__main__":
    main()
